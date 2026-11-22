package main

import (
	"log/slog"
	"os"

	"stravin.dev/core/internal/daemon"
)

func main() {
	if err := daemon.RunWithSignals("stravin-edge", ":8080"); err != nil {
		slog.Error("stravin-edge exited with error", "error", err)
		os.Exit(1)
	}
}
