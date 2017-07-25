# bigsort
Sort a big text file by splitting it into smaller files and merging them

### Usage
```
$ ./bigsort -h
$ ./bigsort bigfile.txt sorted.txt
```

## Requirements
- Golang v1.8 or greater: https://golang.org/doc/install
- Linux amd64: Tested on Linux amd64 only, not sure if it works on Windows due to signal catching.

### Build
`$ make`

## How does it work
The constraint is to keep memory usage under an arbitrary value (BufferSize)

- 1st pass: Splitting the file into smaller sorted temporary files. The following 3 goroutines run in parallel, 2nd pass comes after all of them are done.

  - read_lines: Read bigfile.txt, scan for lines and put them into a slice; the read buffer is 5% of BufferSize. Once the slice has a size of 90% BufferSize send it to the block_ch channel.

  - sort_blocks: Take a block from the block_ch channel, sort it, and sent it to the save_ch channel.
	
	  Heapsort was chosen as the sorting algorithm, it is very simple to implement, it is O(n log n) and in-place. There is also support for using Go's implementation of Quicksort for comparison, although there is no apparent improvement in performance.

	  The most efficient sorting algorithm could be a parallel form of Mergesort, even though the block size have to be less than 90% of BufferSize to account for the extra memory usage.

  - save_blocks: Take a sorted block from the save_ch channel and write to a temporary file; the write buffer is 5% of BufferSize.

- 2nd pass: Merging temporary files into the destination file. The following N + 2 goroutines run in parallel.

  - read_block: Open the N temporary files simultaneously, scan for lines and send them to their channels line_channels[idx]; the read buffer determined by:
    BufferSize/(N + 1). The + 1 is to reserve some memory for the write buffer.

  - merge_lines: Take lines from the line_channels[idx] and put them into a heap. After each insert, pop the last element from the heap and send it to the merge_ch channel.

  - write_file: Take a line from the merge_ch channel and write it to the destination file.


### Notes
Go's underlying memory management is not as straightforward as C from an OS point of view owing to reservations, garbage collection... A memory profile show that  the memory footprint is equal to BufferSize, however, this might not be the case when looking at top or ps output.
