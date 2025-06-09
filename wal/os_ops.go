package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func CreateDirectoryIfNotExists(logDirectory string) error {
	err := os.MkdirAll(logDirectory, 0755)
	if err != nil {
		return err
	}
	return nil
}

func CreateANewSegmentFileIfNotExists(logDirectory string, files []string) error {
	if len(files) <= 0 {
		// create a new segment file
		segmentFile, err := CreateSegmentFile(logDirectory, 0)
		if err != nil {
			return err
		}

		if err := segmentFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

func CreateSegmentFile(directory string, segmentFileNo int) (*os.File, error) {
	filePath := filepath.Join(directory, fmt.Sprintf("%s%d%s", segmentPrefix, segmentFileNo, segmentSuffix))
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	// Initialize the segment header
	header := NewSegmentHeader(uint32(segmentFileNo))
	if err := binary.Write(file, binary.LittleEndian, header.ToBytes()); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write segment header: %w", err)
	}
	return file, nil
}

func OpenSegmentFile(logDirectory string, lastSegmentFileNo int) (*os.File, error) {
	filePath := filepath.Join(logDirectory, fmt.Sprintf("%s%d%s", segmentPrefix, lastSegmentFileNo, segmentSuffix))
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func GetLastSegmentFileNo(files []string) (int, error) {
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

func ReadSegmentFiles(logDirectory string) ([]string, error) {
	fileName := filepath.Join(logDirectory, segmentPrefix+"*"+segmentSuffix)
	files, err := filepath.Glob(fileName)
	if err != nil {
		return nil, err
	}
	return files, err
}

func ReadAllDataLogs(file *os.File) ([]*Wal_Data_Log, error) {
	var dataLogs []*Wal_Data_Log

	fmt.Println("[DEBUG] Reading next data log from file...")
	if _, err := file.Seek(16, io.SeekStart); err != nil {
		return dataLogs, fmt.Errorf("failed to seek past header: %w", err)
	}
	fmt.Println("[DEBUG] Reading next data log from file ends...")

	for {
		var size int32
		err := binary.Read(file, binary.LittleEndian, &size)
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

		if err != nil {
			if err == io.EOF {
				break
			}
			return dataLogs, err
		}

		dataLog, err := UnmarshalAndVerifyDataLog(data)
		if err != nil {
			fmt.Printf("[DEBUG] Failed to unmarshal/verify data log: %v\n", err)
			return dataLogs, err
		}
		dataLogs = append(dataLogs, dataLog)
	}
	return dataLogs, nil
}
