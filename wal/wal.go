package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

const syncInterval = 1 * time.Millisecond
const segmentPrefix = "wal-segment-"
const segmentSuffix = ".wal"

type Wal struct {
	logDirectory        string
	currentSegment      *os.File
	lock                sync.Mutex
	lastLogSequenceNo   uint64
	lastCheckpointLSN   uint64 // Last checkpoint log sequence number
	bufferWriter        *bufio.Writer
	syncTimer           *time.Timer
	triggerFSync        bool
	maxFileSize         int64
	maxSegments         int
	currentSegmentIndex int
	context             context.Context
	cancel              context.CancelFunc
}

func (wal *Wal) CurrentSegmentFileName() string {
	if wal.currentSegment != nil {
		return wal.currentSegment.Name()
	}
	return "<nil>"
}

func (wal *Wal) Write(data []byte) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	wal.lastLogSequenceNo++
	walDataLog := &Wal_Data_Log{
		LogSequenceNumber: wal.lastLogSequenceNo,
		Type:              Log_Type_DATA,
		Data:              data,
		// TODO: change to CRC32 castagnoli, read first and change
		CRC: crc32.ChecksumIEEE(append(data, byte(wal.lastLogSequenceNo))),
	}

	return wal.writeDataLogToBuffer(walDataLog)
}

func (wal *Wal) _rotateLog(currentDataLogSize int32) error {
	fileInfo, err := wal.currentSegment.Stat()
	if err != nil {
		return err
	}

	if fileInfo.Size()+int64(wal.bufferWriter.Buffered())+int64(currentDataLogSize) >= wal.maxFileSize {
		if err := wal.rotateLog(); err != nil {
			return err
		}
	}

	return nil
}

func (wal *Wal) rotateLog() error {
	if err := wal.Sync(false); err != nil {
		return err
	}

	if err := wal.currentSegment.Close(); err != nil {
		return err
	}
	fmt.Printf("[DEBUG] Rotating log: closing segment index: %d\n", wal.currentSegmentIndex)
	wal.currentSegmentIndex++
	if wal.currentSegmentIndex >= wal.maxSegments {
		if err := wal.moveOldestSegmentToArchival(); err != nil {
			return err
		}
	}
	fmt.Printf("[DEBUG] Rotating log: new segment index: %d\n", wal.currentSegmentIndex)
	newFile, err := CreateSegmentFile(wal.logDirectory, wal.currentSegmentIndex)
	if err != nil {
		return err
	}

	wal.currentSegment = newFile
	wal.bufferWriter = bufio.NewWriter(newFile)

	return nil
}

func (wal *Wal) moveOldestSegmentToArchival() error {
	files, err := filepath.Glob(filepath.Join(wal.logDirectory, segmentPrefix+"*"))
	if err != nil {
		return err
	}

	var oldestSegmentFilePath string
	if len(files) > 0 {
		// Find the oldest segment ID
		oldestSegmentFilePath, err = wal.findOldestSegmentFile(files)
		if err != nil {
			return err
		}
	} else {
		return nil
	}

	fmt.Printf("[DEBUG] Moving oldest segment file: %s\n", oldestSegmentFilePath)
	// Move the oldest segment file to archival
	fileName := strings.Split(oldestSegmentFilePath, "/")[len(strings.Split(oldestSegmentFilePath, "/"))-1]
	archivalDirectory := filepath.Join("data", "archival")
	archivalFilePath := filepath.Join(archivalDirectory, fileName)
	if err := os.MkdirAll(archivalDirectory, 0755); err != nil {
		return err
	}
	if err := os.Rename(oldestSegmentFilePath, archivalFilePath); err != nil {
		return err
	}

	return nil
}

func (wal *Wal) findOldestSegmentFile(files []string) (string, error) {
	var oldestSegmentFilePath string
	oldestSegmentID := math.MaxInt64
	for _, file := range files {
		// Get the segment index from the file name
		segmentStr := strings.TrimPrefix(file, filepath.Join(wal.logDirectory, segmentPrefix))
		segmentStr = strings.TrimSuffix(segmentStr, segmentSuffix)
		segmentIndex, err := strconv.Atoi(segmentStr)
		if err != nil {
			return "", err
		}

		if segmentIndex < oldestSegmentID {
			oldestSegmentID = segmentIndex
			oldestSegmentFilePath = file
		}
	}

	return oldestSegmentFilePath, nil
}

func (wal *Wal) writeDataLogToBuffer(walDataLog *Wal_Data_Log) error {
	marshaledData, err := proto.Marshal(walDataLog)
	if err != nil {
		return err
	}
	size := int32(len(marshaledData))

	if err := wal._rotateLog(size); err != nil {
		return err
	}

	if err := binary.Write(wal.bufferWriter, binary.LittleEndian, size); err != nil {
		return err
	}

	if _, err := wal.bufferWriter.Write(marshaledData); err != nil {
		return err
	}
	return nil
}

func NewWal(logDirectory string, maxFileSize int64, maxSegments int, triggerFSync bool) (*Wal, error) {
	CreateDirectoryIfNotExists(logDirectory)
	files, err := ReadSegmentFiles(logDirectory)
	if err != nil {
		return nil, err
	}

	err = CreateANewSegmentFileIfNotExists(logDirectory, files)
	if err != nil {
		return nil, err
	}

	lastSegmentFileNo, err := GetLastSegmentFileNo(files)
	if err != nil {
		return nil, err
	}

	file, err := OpenSegmentFile(logDirectory, lastSegmentFileNo)
	if err != nil {
		return nil, err
	}

	if _, err = file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	context, cancel := context.WithCancel(context.Background())

	wal := &Wal{
		logDirectory:        logDirectory,
		currentSegment:      file,
		lastLogSequenceNo:   0,
		bufferWriter:        bufio.NewWriter(file),
		syncTimer:           time.NewTimer(syncInterval),
		triggerFSync:        triggerFSync,
		maxFileSize:         maxFileSize,
		maxSegments:         maxSegments,
		currentSegmentIndex: lastSegmentFileNo,
		context:             context,
		cancel:              cancel,
	}

	wal.lastLogSequenceNo, err = wal.getLastLogSequenceNo()
	if err != nil {
		return nil, err
	}

	go wal.houseKeeping()
	return wal, nil
}

func (wal *Wal) houseKeeping() {
	for {
		select {
		case <-wal.syncTimer.C:

			wal.lock.Lock()
			err := wal.Sync(true)
			wal.lock.Unlock()

			if err != nil {
				fmt.Printf("Error while performing sync: %v", err)
			}

		case <-wal.context.Done():
			return
		}
	}
}

func (wal *Wal) Sync(shouldCheckpointed bool) error {
	if err := wal.bufferWriter.Flush(); err != nil {
		return err
	}
	if wal.triggerFSync {
		if err := wal.currentSegment.Sync(); err != nil {
			return err
		}

		if shouldCheckpointed {
			wal.checkpoint()
		}
	}

	wal.resetTimer()

	return nil
}

func (wal *Wal) resetTimer() {
	wal.syncTimer.Reset(syncInterval)
}

func (wal *Wal) getLastLogSequenceNo() (uint64, error) {
	file, err := os.OpenFile(wal.currentSegment.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var lastDataLog *Wal_Data_Log

	if _, err := file.Seek(16, io.SeekStart); err != nil {
		panic(err)
	}

	for {
		var size int32
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}

		if size <= 0 {
			break
		}

		data := make([]byte, size)
		if _, err := io.ReadFull(file, data); err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}

		walDataLog, err := UnmarshalAndVerifyDataLog(data)
		if err != nil {
			return 0, err
		}

		lastDataLog = walDataLog
	}

	if lastDataLog == nil {
		return 0, nil
	}
	return lastDataLog.LogSequenceNumber, nil
}

func (wal *Wal) Close() error {
	wal.cancel()
	if err := wal.Sync(true); err != nil {
		return err
	}
	return wal.currentSegment.Close()
}

func (wal *Wal) ReadCurrentSegmentFile() ([]*Wal_Data_Log, error) {
	file, err := os.OpenFile(wal.currentSegment.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	dataLogs, err := ReadAllDataLogs(file)
	if err != nil {
		return nil, err
	}

	return dataLogs, nil
}

func (wal *Wal) checkpoint() {
	// After successful disk sync, write a checkpoint record
	wal.lastLogSequenceNo++
	wal.lastCheckpointLSN = wal.lastLogSequenceNo
	checkpointLog := &Wal_Data_Log{
		LogSequenceNumber: wal.lastLogSequenceNo,
		Type:              Log_Type_CHECKPOINT,
		Data:              []byte{}, // Empty data for checkpoint
		CRC:               crc32.ChecksumIEEE([]byte{byte(wal.lastLogSequenceNo)}),
	}

	if err := wal.writeDataLogToBuffer(checkpointLog); err != nil {
		fmt.Printf("failed to checkpoint: %v\n", err)
		return
	}
}

// Returns last checkpoint record and segment file number containing it
func (wal *Wal) getLastCheckPointLSNWalEntry() (*Wal_Data_Log, int, error) {
	// Start from current segment index and work backwards
	for segmentIndex := wal.currentSegmentIndex; segmentIndex >= 0; segmentIndex-- {
		// Open segment file
		segmentPath := filepath.Join(wal.logDirectory, fmt.Sprintf("%s%d%s", segmentPrefix, segmentIndex, segmentSuffix))
		file, err := os.OpenFile(segmentPath, os.O_RDONLY, 0644)
		if err != nil {
			if os.IsNotExist(err) {
				// Try archive directory
				segmentPath = filepath.Join("data", "archival", fmt.Sprintf("%s%d%s", segmentPrefix, segmentIndex, segmentSuffix))
				file, err = os.OpenFile(segmentPath, os.O_RDONLY, 0644)
				if err != nil {
					continue // Skip if not found in either location
				}
			} else {
				return nil, 0, err
			}
		}
		defer file.Close()

		// Read all records from this segment
		records, err := ReadAllDataLogs(file)
		if err != nil {
			return nil, 0, err
		}

		// Search backwards for checkpoint
		for i := len(records) - 1; i >= 0; i-- {
			if records[i].Type == Log_Type_CHECKPOINT {
				return records[i], segmentIndex, nil
			}
		}
	}
	return nil, 0, fmt.Errorf("no checkpoint found")
}

func (wal *Wal) RecoverFromCheckpoint() error {
	// Find last checkpoint
	checkpoint, segmentIndex, err := wal.getLastCheckPointLSNWalEntry()
	if err != nil {
		return err
	}

	fmt.Printf("Found Recovery checkpoint at LSN %d in segment %d\n", checkpoint.LogSequenceNumber, segmentIndex)

	// Read and print all records from checkpoint onwards
	for currSegment := segmentIndex; currSegment <= wal.currentSegmentIndex; currSegment++ {
		// Try active directory first
		segmentPath := filepath.Join(wal.logDirectory, fmt.Sprintf("%s%d%s", segmentPrefix, currSegment, segmentSuffix))
		file, err := os.OpenFile(segmentPath, os.O_RDONLY, 0644)
		if err != nil {
			if os.IsNotExist(err) {
				// Try archive directory
				segmentPath = filepath.Join("data", "archival", fmt.Sprintf("%s%d%s", segmentPrefix, currSegment, segmentSuffix))
				file, err = os.OpenFile(segmentPath, os.O_RDONLY, 0644)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
		defer file.Close()

		records, err := ReadAllDataLogs(file)
		if err != nil {
			return err
		}

		// Print records after checkpoint LSN
		for _, record := range records {
			if record.LogSequenceNumber > checkpoint.LogSequenceNumber {
				fmt.Printf("Replay record: LSN=%d, Type=%v, Data=%s\n",
					record.LogSequenceNumber, record.Type, string(record.Data))
			}
		}
	}

	return nil
}
