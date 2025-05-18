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
		Data:              data,
		// change to CRC32 castagnoli
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
	if err := wal.Sync(); err != nil {
		return err
	}

	if err := wal.currentSegment.Close(); err != nil {
		return err
	}
	fmt.Printf("[DEBUG] Rotating log: closing segment index: %d\n", wal.currentSegmentIndex)
	wal.currentSegmentIndex++
	if wal.currentSegmentIndex >= wal.maxSegments {
		if err := wal.deleteOldestSegment(); err != nil {
			return err
		}
	}
	fmt.Printf("[DEBUG] Rotating log: new segment index: %d\n", wal.currentSegmentIndex)
	newFile, err := createSegmentFile(wal.logDirectory, wal.currentSegmentIndex)
	if err != nil {
		return err
	}

	wal.currentSegment = newFile
	wal.bufferWriter = bufio.NewWriter(newFile)

	return nil
}

func (wal *Wal) deleteOldestSegment() error {
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

	// Delete the oldest segment file
	if err := os.Remove(oldestSegmentFilePath); err != nil {
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
	createDirectoryIfNotExists(logDirectory)
	files, err := readSegmentFiles(logDirectory)
	if err != nil {
		return nil, err
	}

	err = createANewSegmentFileIfNotExists(logDirectory, files)
	if err != nil {
		return nil, err
	}

	lastSegmentFileNo, err := getLastSegmentFileNo(files)
	if err != nil {
		return nil, err
	}

	file, err := openSegmentFile(logDirectory, lastSegmentFileNo)
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
			err := wal.Sync()
			wal.lock.Unlock()

			if err != nil {
				fmt.Printf("Error while performing sync: %v", err)
			}

		case <-wal.context.Done():
			return
		}
	}
}

func (wal *Wal) Sync() error {
	if err := wal.bufferWriter.Flush(); err != nil {
		return err
	}
	if wal.triggerFSync {
		if err := wal.currentSegment.Sync(); err != nil {
			return err
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

		walDataLog, err := unmarshalAndVerifyDataLog(data)
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

func unmarshalAndVerifyDataLog(data []byte) (*Wal_Data_Log, error) {
	var walDataLog Wal_Data_Log
	if err := proto.Unmarshal(data, &walDataLog); err != nil {
		return nil, err
	}
	if validDataIntegrity(&walDataLog) {
		return &walDataLog, nil
	} else {
		return nil, fmt.Errorf("data integrity check failed")
	}

}

func openSegmentFile(logDirectory string, lastSegmentFileNo int) (*os.File, error) {
	filePath := filepath.Join(logDirectory, fmt.Sprintf("%s%d%s", segmentPrefix, lastSegmentFileNo, segmentSuffix))
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func validDataIntegrity(walDataLog *Wal_Data_Log) bool {
	return walDataLog.CRC == crc32.ChecksumIEEE(append(walDataLog.Data, byte(walDataLog.LogSequenceNumber)))
}

func createANewSegmentFileIfNotExists(logDirectory string, files []string) error {
	if len(files) <= 0 {
		// create a new segment file
		segmentFile, err := createSegmentFile(logDirectory, 0)
		if err != nil {
			return err
		}

		if err := segmentFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

func getLastSegmentFileNo(files []string) (int, error) {
	var lastSegmentFileNo int
	for _, file := range files {
		_, fileName := filepath.Split(file)
		segmentString := strings.TrimPrefix(fileName, segmentPrefix)
		segmentString = strings.TrimSuffix(segmentString, segmentSuffix)
		currentSegmentFileNo, err := strconv.Atoi(segmentString)
		if err != nil {
			return 0, err
		}
		if currentSegmentFileNo > lastSegmentFileNo {
			lastSegmentFileNo = currentSegmentFileNo
		}
	}
	return lastSegmentFileNo, nil
}

func createSegmentFile(directory string, segmentFileNo int) (*os.File, error) {
	filePath := filepath.Join(directory, fmt.Sprintf("%s%d%s", segmentPrefix, segmentFileNo, segmentSuffix))
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func readSegmentFiles(logDirectory string) ([]string, error) {
	fileName := filepath.Join(logDirectory, segmentPrefix+"*"+segmentSuffix)
	files, err := filepath.Glob(fileName)
	if err != nil {
		return nil, err
	}
	return files, err
}

func createDirectoryIfNotExists(logDirectory string) error {
	err := os.MkdirAll(logDirectory, 0755)
	if err != nil {
		return err
	}
	return nil
}

func (wal *Wal) Close() error {
	wal.cancel()
	if err := wal.Sync(); err != nil {
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

	dataLogs, err := readAllDataLogs(file)
	if err != nil {
		return nil, err
	}

	return dataLogs, nil
}

func readAllDataLogs(file *os.File) ([]*Wal_Data_Log, error) {
	var dataLogs []*Wal_Data_Log
	for {
		var size int32
		err := binary.Read(file, binary.LittleEndian, &size)
		fmt.Printf("[DEBUG] Read size: %d, err: %v\n", size, err)
		if err != nil {
			if err == io.EOF {
				break
			}
			return dataLogs, err
		}

		if size <= 0 {
			fmt.Println("[DEBUG] Encountered non-positive size, breaking loop.")
			break
		}

		data := make([]byte, size)
		_, err = io.ReadFull(file, data)
		fmt.Printf("[DEBUG] ReadFull for %d bytes, err: %v\n", size, err)
		if err != nil {
			if err == io.EOF {
				break
			}
			return dataLogs, err
		}

		dataLog, err := unmarshalAndVerifyDataLog(data)
		if err != nil {
			fmt.Printf("[DEBUG] Failed to unmarshal/verify data log: %v\n", err)
			return dataLogs, err
		}
		fmt.Printf("[DEBUG] Successfully read data log: SeqNo=%d\n", dataLog.LogSequenceNumber)
		dataLogs = append(dataLogs, dataLog)
	}
	return dataLogs, nil
}
