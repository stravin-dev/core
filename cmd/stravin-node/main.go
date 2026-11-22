package main

import (
	"log/slog"
	"os"

	"stravin.dev/core/internal/node"
)

func main() {
	if err := node.RunWithSignals(); err != nil {
		slog.Error("stravin-node exited with error", "error", err)
		os.Exit(1)
	}
}
