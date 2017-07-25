package main

import (
    "fmt"
    "os"
    "time"
)


func Println(args ...interface{}) {
    fmt.Fprintln(os.Stdout, args...)
}

func Printf(format string, args ...interface{}) {
    fmt.Fprintf(os.Stdout, format, args...)
}

func Stderrln(args ...interface{}) {
    fmt.Fprintln(os.Stderr, args...)
}

func Stderrf(format string, args ...interface{}) {
    fmt.Fprintf(os.Stderr, format, args...)
}


func Iserror(err error, args ...interface{}) bool {
    if err != nil {
        Stderrf("%s. ", err)
        Stderrln(args...)

        return true
    }

    return false
}

func Sleep(seconds int) {
    time.Sleep(time.Duration(seconds) * time.Second)
}

func Sleepms(mseconds int) {
    time.Sleep(time.Duration(mseconds) * time.Millisecond)
}
