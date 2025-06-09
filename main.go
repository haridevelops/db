package main

import (
	"fmt"
	wal "wal/Wal"
)

func main() {
	w, err := wal.NewWal("data/log", 5*1000*1000 /*1MB or 10^6*/, 3, true)
	if err != nil {
		panic(err)
	}
	read(w)

	close(w)
}

func read(w *wal.Wal) {
	fmt.Printf("[DEBUG] Reading from segment file: %s\n", w.CurrentSegmentFileName())
	dataLogs, err := w.ReadCurrentSegmentFile()
	if err != nil {
		panic(err)
	}
	for _, dataLog := range dataLogs {
		fmt.Printf("Read key=%s value=%s type=%s\n", dataLog.LogSequenceNumber, string(dataLog.Data), dataLog.Type)
	}
	fmt.Printf("Read all data logs %d\n", len(dataLogs))
}

func close(w *wal.Wal) {
	// Close the WAL
	if err := w.Close(); err != nil {
		panic(err)
	}
	fmt.Println("WAL closed")
}
