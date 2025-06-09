package wal

import (
	"fmt"
	"hash/crc32"

	"google.golang.org/protobuf/proto"
)

func ValidDataIntegrity(walDataLog *Wal_Data_Log) bool {
	return walDataLog.CRC == crc32.ChecksumIEEE(append(walDataLog.Data, byte(walDataLog.LogSequenceNumber)))
}

func UnmarshalAndVerifyDataLog(data []byte) (*Wal_Data_Log, error) {
	var walDataLog Wal_Data_Log
	if err := proto.Unmarshal(data, &walDataLog); err != nil {
		return nil, err
	}
	if ValidDataIntegrity(&walDataLog) {
		return &walDataLog, nil
	} else {
		return nil, fmt.Errorf("data integrity check failed")
	}

}
