package main

import (
    "os"
    "bufio"
    "time"
    "sort"
    "strconv"
    "sync"
    "sync/atomic"
    "container/heap"
)


const (
    BLOCK_BUFFER_SIZE = 0
    CACHE_BUFFER_SIZE = 0
    LINE_BUFFER_SIZE = 8

    RW_BUFFER_PERCENTAGE = 5
    BLOCK_PERCENTAGE = 90

    SLEEP_TIME_MS =    100
    CACHE_PREFIX =     "cache/block_"
)


var memory_buffer_size int64 = 0

// Times
var iowait_read = time.Duration(0)
var iowait_write = time.Duration(0)
var time_slept = time.Duration(0)
var time_sorting = time.Duration(0)
var time_merging = time.Duration(0)

func read_lines(src_file *os.File, block_ch chan<- *[]string) {
    defer src_file.Close()
    defer close(block_ch)

    var buffer_size int64 = (Options.BufferSize/100) * RW_BUFFER_PERCENTAGE
    var block_size int64 = (Options.BufferSize/100) * BLOCK_PERCENTAGE

    scanner := bufio.NewScanner(src_file)
    scanner.Buffer(make([]byte, buffer_size), 0)

    var block *[]string
    var block_buffer_size int64

    // Stop reading file on sigINT
    for !sigINT {
        // Sleep if memory buffer is full
        for atomic.LoadInt64(&memory_buffer_size) >= block_size {
            start := time.Now()
                Sleepms(SLEEP_TIME_MS)
            time_slept += time.Since(start)
        }

        start := time.Now()
            scan := scanner.Scan()
        iowait_read += time.Since(start)

        // EOF (or error)
        if !scan {
            break
        }

        // Create new block
        if block == nil {
            block = new([]string)
            block_buffer_size = 0
        }

        // Append line
        *block = append(*block, scanner.Text())
        block_buffer_size += int64(len(scanner.Bytes()))
        atomic.AddInt64(&memory_buffer_size, int64(len(scanner.Bytes())))

        if block_buffer_size >= block_size {
            block_ch <- block
            block = nil
        }
    }

    // Check for errors
    if err := scanner.Err(); err != nil {
        Stderrln("Error:", err)  // TODO: Proper error handling
    }

    // Send last block to channel if not empty
    if len(*block) > 0 {
        block_ch <- block
        block = nil
    }

}

func sort_blocks(block_ch <-chan *[]string, save_ch chan<- *[]string) {
    defer close(save_ch)

    for block := range block_ch {
        start := time.Now()
            if Options.Quicksort {
                sort.Sort(Alphabetical(*block))
            } else {
                // TODO: Perhaps a parallel mergesort (with a smaller block to account for
                // the extra memory) would be more efficient, but heapsort is good enough IMO
                Heapsort(Alphabetical(*block))
            }
        time_sorting += time.Since(start)
        save_ch <- block
    }

}

func save_blocks(temp_files *[]string, save_ch <-chan *[]string, wg *sync.WaitGroup) {
    defer wg.Done()

    var line string
    buffer_size := (int(Options.BufferSize)/100) * RW_BUFFER_PERCENTAGE

    idx := 0
    for block := range save_ch {
        // Create temp file
        temp_file, err := os.Create(CACHE_PREFIX + strconv.Itoa(idx))
        if err != nil {
            Stderrln("Error:", err)  // TODO: Proper error handling
        }
        writer := bufio.NewWriterSize(temp_file, buffer_size)

        // Keep track of temp file
        *temp_files = append(*temp_files, temp_file.Name())

        for len(*block) > 0 {
            line = (*block)[0]
            *block = (*block)[1:]

            // Write line to temp file
            start := time.Now()
                writer.WriteString(line)
                writer.WriteString("\n")
            iowait_write += time.Since(start)

            atomic.AddInt64(&memory_buffer_size, int64(-1 * len(line)))
        }

        // Empty the buffer
        start := time.Now()
            writer.Flush()
        iowait_write += time.Since(start)

        temp_file.Close()

        idx++
    }

}

func read_block(temp_file *os.File, buffer_size int, line_ch chan<- string) {
    defer temp_file.Close()
    defer close(line_ch)

    scanner := bufio.NewScanner(temp_file)
    scanner.Buffer(make([]byte, buffer_size), 0)

    // Stop reading file on sigINT
    for !sigINT {
        start := time.Now()
            scan := scanner.Scan()
        iowait_read += time.Since(start)

        // EOF (or error)
        if !scan {
            break
        }

        // Append line
        line_ch <- scanner.Text()
    }

    // Check for errors
    if err := scanner.Err(); err != nil {
        Stderrln("Error:", err)  // TODO: Proper error handling
    }

}

func merge_lines(line_channels map[int]chan string, merge_ch chan<- string) {
    defer close(merge_ch)

    var l_heap LineHeap
    heap.Init(&l_heap)

    // Add one element from each channel to the heap
    for idx, line_ch := range line_channels {
        line := Line{value: <-line_ch, idx: idx}
        start := time.Now()
            heap.Push(&l_heap, line)
        time_merging += time.Since(start)
    }

    for l_heap.Len() > 0 {
        //Stderrf("%+v\n", heap.Pop(&l_heap))
        first_line := heap.Pop(&l_heap).(Line)
        merge_ch <- first_line.value

        line, ok := <-line_channels[first_line.idx]
        if ok {
            new_line := Line{value: line, idx: first_line.idx}
            start := time.Now()
                heap.Push(&l_heap, new_line)
            time_merging += time.Since(start)
        }

    }

}

func write_file(dest_file *os.File, buffer_size int, merge_ch <-chan string, wg *sync.WaitGroup) {
    defer wg.Done()
    defer dest_file.Close()

    writer := bufio.NewWriterSize(dest_file, buffer_size)

    for line := range merge_ch {
        // Write line to dest file
        start := time.Now()
            writer.WriteString(line)
            writer.WriteString("\n")
        iowait_write += time.Since(start)

    }

    // Empty the buffer
    start := time.Now()
        writer.Flush()
    iowait_write += time.Since(start)

}

// Main section ########################################################################################################
func App() error {
    var wg sync.WaitGroup
    var temp_files []string
    block_ch := make(chan *[]string, BLOCK_BUFFER_SIZE)
    save_ch := make(chan *[]string, CACHE_BUFFER_SIZE)
    line_channels := make(map[int]chan string)
    merge_ch := make(chan string, LINE_BUFFER_SIZE)


    // Open source file
    src_file, err := os.Open(Options.SourceFile)
    if Iserror(err) {
        return err
    }

    // Read lines into blocks, from the source file
    go read_lines(src_file, block_ch)

    // Sort the lines in the blocks
    go sort_blocks(block_ch, save_ch)

    // Save blocks to disk into temporary files
    wg.Add(1)
    go save_blocks(&temp_files, save_ch, &wg)
    Stderrln("1st pass: Splitting the file into smaller sorted temporary files")
    wg.Wait()

    // Read lines from sorted blocks, put them in channels
    buffer_size := int(Options.BufferSize)/(len(temp_files) + 1)
    for idx, temp_file_path := range temp_files {
        line_channels[idx] = make(chan string, LINE_BUFFER_SIZE)

        // Open temp file
        temp_file, err := os.Open(temp_file_path)
        if Iserror(err) {
            return err
        }
        go read_block(temp_file, buffer_size, line_channels[idx])
    }

    // Mergesort lines
    go merge_lines(line_channels, merge_ch)

    // Open destination file
    dest_file, err := os.Create(Options.DestFile)
    if Iserror(err) {
        return err
    }
    // Write output file
    wg.Add(1)
    go write_file(dest_file, buffer_size, merge_ch, &wg)
    Stderrln("2nd pass: Merging temporary files into dest_file")
    wg.Wait()

    // Cleanup
    for _, file := range temp_files {
        if !Options.KeepTemps {
            os.Remove(file)
        }
    }

    // Print timings
    if Verbose {
        Stderrln("\n(not fully accurate, just for reference)")
        Stderrln("Read iowait: ", iowait_read)
        Stderrln("Write iowait: ", iowait_write)
        Stderrln("Sleep time: ", time_slept)
        Stderrln("Sorting time: ", time_sorting)
        Stderrln("Merging time: ", time_merging)
    }

    return nil
}
