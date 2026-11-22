package main

import (
	"log/slog"
	"os"

	"stravin.dev/core/internal/daemon"
)

func main() {
	if err := daemon.RunWithSignals("stravin-node", ":8081"); err != nil {
		slog.Error("stravin-node exited with error", "error", err)
		os.Exit(1)
	}
}
