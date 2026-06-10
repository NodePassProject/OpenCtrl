// Command openctrl starts an OpenCtrl core from a URL-shaped configuration.
//
// The binary uses the URL scheme as the core selector. Today only master:// is
// handled here; child protocol URLs are passed to managed child binaries by the
// master package and are not parsed by the CLI entrypoint.
package main

import (
	"fmt"
	"net/url"
	"os"
	"runtime"

	"openctrl/v2/internal/master"
)

// version is replaced by release builds and remains "dev" for local builds.
//
// Keep this variable package-level so linker flags can override it without
// changing code.
var version = "dev"

func main() {
	if err := start(os.Args); err != nil {
		exit(err)
	}
}

// start validates the CLI contract, parses the configuration URL, and runs the
// selected core until it exits.
//
// The function is kept separate from main so tests can exercise argument
// handling without terminating the process through os.Exit.
func start(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("Main.start: missing configuration URL argument")
	}

	switch args[1] {
	case "help", "--help", "-h":
		fmt.Printf("openctrl <configuration URL>\n")
		return nil
	case "version", "--version", "-v":
		fmt.Printf("openctrl-%s %s/%s\n", version, runtime.GOOS, runtime.GOARCH)
		return nil
	}

	parsedURL, err := url.Parse(args[1])
	if err != nil {
		return fmt.Errorf("Main.start: parse command failed: %w", err)
	}

	core, err := createCore(parsedURL)
	if err != nil {
		return fmt.Errorf("Main.start: create core failed: %w", err)
	}

	core.Run()
	return nil
}

// exit prints the final runtime envelope before terminating with a non-zero
// status code.
//
// Startup failures are intentionally one-line and machine-readable enough for
// service managers while still being readable in a terminal.
func exit(err error) {
	errMsg := "none"
	if err != nil {
		errMsg = err.Error()
	}

	fmt.Fprintf(os.Stderr,
		"openctrl-%s %s/%s pid=%d error=%s\n",
		version, runtime.GOOS, runtime.GOARCH, os.Getpid(), errMsg)
	os.Exit(1)
}

// createCore maps the configuration scheme to the matching OpenCtrl core.
//
// This is the top-level dispatch boundary. Adding another core should happen
// here, while adding another managed child protocol should generally not; child
// protocols belong behind the master binary launch contract.
func createCore(parsedURL *url.URL) (interface{ Run() }, error) {
	switch parsedURL.Scheme {
	case "master":
		return master.NewMaster(parsedURL, version)
	default:
		return nil, fmt.Errorf("Main.createCore: unknown core: %v", parsedURL)
	}
}
