package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	wal "wal/Wal"
)

func main() {
	w, err := wal.NewWal("data/log", 1*1000*1000 /*1MB or 10^6*/, 3, true)
	if err != nil {
		panic(err)
	}

	write(w, 80000)
	read(w)

	close(w)
}

func write(w *wal.Wal, noOfWrites int) {
	prefixKey := "prefix-str"
	for i := 0; i < noOfWrites; i++ {
		key := uint32(i)
		keyStr := strconv.FormatUint(uint64(key), 10)
		valueObj := map[string]any{
			"Operation": "insert",
			"key":       prefixKey + strconv.Itoa(i),
			"value": map[string]any{
				"paymentStatus": "success",
				"paymentAmount": 100 + (i * 10),
			},
		}
		valueBytes, err := json.Marshal(valueObj)
		if err != nil {
			panic(err)
		}
		if err := w.Write([]byte(fmt.Sprintf("%s:%s", keyStr, string(valueBytes)))); err != nil {
			panic(err)
		}
		// fmt.Printf("Wrote key=%s value=%s\n", keyStr, string(valueBytes))
	}
}

func read(w *wal.Wal) {
	fmt.Printf("[DEBUG] Reading from segment file: %s\n", w.CurrentSegmentFileName())
	dataLogs, err := w.ReadCurrentSegmentFile()
	if err != nil {
		panic(err)
	}
	for _, dataLog := range dataLogs {
		fmt.Printf("Read key=%s value=%s\n", dataLog.LogSequenceNumber, string(dataLog.Data))
	}
	fmt.Println("Read all data logs")
}

func close(w *wal.Wal) {
	// Close the WAL
	if err := w.Close(); err != nil {
		panic(err)
	}
	fmt.Println("WAL closed")
}
