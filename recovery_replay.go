package main

import (
	"fmt"
	"wal/wal"
)

func main() {
	w, err := wal.NewWal("data/log", 5*1000*1000 /*1MB or 10^6*/, 3, false)
	if err != nil {
		panic(err)
	}

	// Perform recovery and replay
	if err := recoveryAndReplay(w); err != nil {
		w.Close()
		panic(err)
	}

	// Close the WAL after recovery is complete
	if err := w.Close(); err != nil {
		panic(err)
	}
}

func recoveryAndReplay(w *wal.Wal) error {
	if err := w.RecoverFromCheckpoint(); err != nil {
		return fmt.Errorf("failed to recover from checkpoint: %v", err)
	}
	return nil
}
