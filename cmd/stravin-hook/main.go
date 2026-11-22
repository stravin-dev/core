package main

import (
	"log/slog"
	"os"

	"stravin.dev/core/internal/daemon"
)

func main() {
	if err := daemon.RunWithSignals("stravin-hook", ":8084"); err != nil {
		slog.Error("stravin-hook exited with error", "error", err)
		os.Exit(1)
	}
}
