package main

import (
    "os"
    "os/signal"
    "runtime"
    //"github.com/pkg/profile"
)

const (
    OK          = 0
    GEN_ERROR   = 1
    PARSE_ERROR = 2
    IO_ERROR    = 3
    SIGINT      = 130
)

var sigINT = false
var signals = make(chan os.Signal)
func process_signals() {
    for sig := range signals {
        if !sigINT && sig == os.Interrupt {
            Stderrln(" [ Interrupt received, cleaning up ... ]")
            sigINT = true
        }
    }
}

var GoProcs int
func init() {
    // Use all CPU cores
    GoProcs = runtime.NumCPU() + 1
    runtime.GOMAXPROCS(GoProcs)

}

func main() {
    //profiling := profile.Start(profile.MemProfile, profile.ProfilePath("cache"), profile.MemProfileRate(16 * 1024))
    //profiling := profile.Start(profile.CPUProfile, profile.ProfilePath("cache"))

    // Process command line arguments
    if err := Process_opts(); Iserror(err) {
        os.Exit(PARSE_ERROR)
    }

    // Print Version and options if verbose
    if Verbose {
        Stderrln(NAME, VERSION)
        Stderrf("Options: %+v\n", Options)
        Stderrln("Golang processes: ", GoProcs)
        Stderrln("")
    }

    // Signal handlers
    go process_signals()
    signal.Notify(signals, os.Interrupt) // SIGINT (Ctrl+C)

    // Run app
    status := App()

    // Exit status handling
    if _, err := status.(*os.PathError); err {
        os.Exit(IO_ERROR)
    }
    if status != nil {
        os.Exit(GEN_ERROR)
    }
    if sigINT {
        os.Exit(SIGINT)
    }

    //profiling.Stop()
    os.Exit(OK)
}
