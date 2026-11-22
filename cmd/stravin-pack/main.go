package main

import (
	"log/slog"
	"os"

	"stravin.dev/core/internal/daemon"
)

func main() {
	if err := daemon.RunWithSignals("stravin-pack", ":8082"); err != nil {
		slog.Error("stravin-pack exited with error", "error", err)
		os.Exit(1)
	}
}
