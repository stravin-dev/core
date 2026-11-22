package main

import (
	"log/slog"
	"os"

	"stravin.dev/core/internal/daemon"
)

func main() {
	if err := daemon.RunWithSignals("stravin-ctrl", ":8083"); err != nil {
		slog.Error("stravin-ctrl exited with error", "error", err)
		os.Exit(1)
	}
}
