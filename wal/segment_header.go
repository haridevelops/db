package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"time"
)

var (
	ErrInvalidHeaderSize = errors.New("invalid header size")
	ErrInvalidChecksum   = errors.New("invalid header checksum")
)

const HeaderSize = 16 // SegmentID (4) + CreatedAt (8) + Checksum (4)

type SegmentHeader struct {
	SegmentID      uint32 // 4 bytes - Current segment identifier
	CreatedAt      int64  // 8 bytes - Creation timestamp
	HeaderChecksum uint32 // 4 bytes - CRC32 of above fields
}

func NewSegmentHeader(segmentID uint32) *SegmentHeader {
	header := &SegmentHeader{
		SegmentID: segmentID,
		CreatedAt: time.Now().UnixNano(),
	}
	header.HeaderChecksum = header.calculateChecksum()
	return header
}

func (h *SegmentHeader) calculateChecksum() uint32 {
	buf := make([]byte, 12) // 4 + 8 bytes (SegmentID + CreatedAt)
	binary.LittleEndian.PutUint32(buf[0:], h.SegmentID)
	binary.LittleEndian.PutUint64(buf[4:], uint64(h.CreatedAt))
	return crc32.ChecksumIEEE(buf)
}

// Serialize header to bytes
func (h *SegmentHeader) ToBytes() []byte {
	buf := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint32(buf[0:], h.SegmentID)
	binary.LittleEndian.PutUint64(buf[4:], uint64(h.CreatedAt))
	binary.LittleEndian.PutUint32(buf[12:], h.HeaderChecksum)
	return buf
}

// Deserialize bytes to header
func ParseSegmentHeader(buf []byte) (*SegmentHeader, error) {
	if len(buf) < HeaderSize {
		return nil, ErrInvalidHeaderSize
	}

	header := &SegmentHeader{
		SegmentID:      binary.LittleEndian.Uint32(buf[0:]),
		CreatedAt:      int64(binary.LittleEndian.Uint64(buf[4:])),
		HeaderChecksum: binary.LittleEndian.Uint32(buf[12:]),
	}

	// Verify checksum
	expectedChecksum := header.calculateChecksum()
	if expectedChecksum != header.HeaderChecksum {
		return nil, ErrInvalidChecksum
	}

	return header, nil
}
