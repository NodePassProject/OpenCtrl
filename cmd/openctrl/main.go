package main

import (
	"fmt"
	"net/url"
	"os"
	"runtime"

	"openctrl/v2/internal/master"
)

var version = "dev"

func main() {
	if err := start(os.Args); err != nil {
		exit(err)
	}
}

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

func createCore(parsedURL *url.URL) (interface{ Run() }, error) {
	switch parsedURL.Scheme {
	case "master":
		return master.NewMaster(parsedURL, version)
	default:
		return nil, fmt.Errorf("Main.createCore: unknown core: %v", parsedURL)
	}
}
