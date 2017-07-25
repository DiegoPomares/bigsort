package main

import (
    "errors"
    "flag"
    "fmt"
    "os"
    "strconv"
    "strings"
)

const (
    NAME    = "bigsort"
    VERSION = "0.1"
)

var Verbose bool

var Options struct {
    SourceFile string
    DestFile   string

    BufferSize int64
    KeepTemps  bool
    Quicksort  bool
}

var DefaultBufferSize int64 = (100 * 1024 * 1024)

var usage = fmt.Sprintf(`%s %s
Usage: %s [options] source_file dest_file

Sort a big text file by spliting it into smaller files and merging them
https://github.com/DiegoPomares/bigsort

Options:
    -b, --buffer-size <bytes>       Buffer size for file operations
                                    Supports multiplicative suffixes KM
                                    (default: 100K)
    -k, --keep-temp-files           Preserve temporary files
    -q, --quicksort                 Use Go's quicksort implementation for the
                                    1st pass instead of mergesort

Miscellaneous:
    -v, --verbose                   Print debugging messages
    -h, --help                      Show this message

`, NAME, VERSION, NAME)

func print_usage() {
    fmt.Printf("%s", usage)
    os.Exit(0)
}

func arg_get(args []string, n int) string {
    if len(args) > n {
        return args[n]
    }

    return ""
}

func Process_opts() error {
    var block_size_human string

    // Flags
    flag.BoolVar(&Verbose, "v", false, "Verbose")
    flag.BoolVar(&Verbose, "verbose", false, "Verbose")

    flag.StringVar(&block_size_human, "b", "", "Buffer size")
    flag.StringVar(&block_size_human, "buffer-size", "", "Buffer size")

    flag.BoolVar(&Options.KeepTemps, "k", false, "Keep temporary files")
    flag.BoolVar(&Options.KeepTemps, "keep-temp-files", false, "Keep temporary files")

    flag.BoolVar(&Options.Quicksort, "q", false, "Use quicksort")
    flag.BoolVar(&Options.Quicksort, "quicksort", false, "Use quicksort")

    flag.Usage = print_usage
    flag.Parse()

    // Positional arguments
    Options.SourceFile = arg_get(flag.Args(), 0)
    Options.DestFile = arg_get(flag.Args(), 1)

    // Check options
    var block_size int64 = DefaultBufferSize
    if block_size_human != "" {
        var err error
        block_size, err = parse_num(block_size_human)
        if err != nil {
            return err
        }

    }

    if Options.SourceFile == "" {
        return errors.New("Missing argument: source_file")
    }
    if Options.DestFile == "" {
        return errors.New("Missing argument: dest_file")
    }

    // Merge default opts
    Options.BufferSize = block_size

    return nil
}

func parse_num(s string) (int64, error) {
    var multi int64 = 1
    s = strings.ToUpper(s)

    switch s[len(s)-1:] {
    case "K":
        multi = 1024
    case "M":
        multi = 1024 * 1024
    case "G":
        multi = 1024 * 1024 * 1024
    case "T":
        multi = 1024 * 1024 * 1024 * 1024
    default:
        s = s + " "
    }

    out, err := strconv.Atoi(s[:len(s)-1])

    return int64(out) * multi, err
}
