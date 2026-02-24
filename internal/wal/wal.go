package wal

import (
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/thirawat27/kvi/pkg/types"
)

type LogEntry struct {
	LSN       uint64          `json:"lsn"`
	Timestamp int64           `json:"timestamp"`
	Op        types.Operation `json:"op"`
	Key       string          `json:"key"`
	Record    *types.Record   `json:"record"`
	Checksum  uint32          `json:"checksum"`
}

type WAL struct {
	dir      string
	file     *os.File
	buffer   []*LogEntry
	mu       sync.Mutex
	lastLSN  uint64
	offset   int64
	batchCap int
}

func NewWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	path := filepath.Join(dir, "kvi.wal")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return &WAL{
		dir:      dir,
		file:     file,
		buffer:   make([]*LogEntry, 0),
		batchCap: 1000,
		offset:   stat.Size(),
	}, nil
}

func (w *WAL) WriteEntry(op types.Operation, key string, rec *types.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.lastLSN++
	entry := &LogEntry{
		LSN:       w.lastLSN,
		Timestamp: time.Now().UnixNano(),
		Op:        op,
		Key:       key,
		Record:    rec,
	}

	// Calculate CRC32 excluding Checksum field obviously
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	entry.Checksum = crc32.ChecksumIEEE(data)

	w.buffer = append(w.buffer, entry)

	// Batch flush
	if len(w.buffer) >= w.batchCap {
		return w.flushUnlocked()
	}

	return nil
}

func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushUnlocked()
}

func (w *WAL) flushUnlocked() error {
	if len(w.buffer) == 0 {
		return nil
	}

	for _, entry := range w.buffer {
		data, err := json.Marshal(entry)
		if err != nil {
			return err
		}

		// Length prefix
		var lengthBuf [4]byte
		binary.LittleEndian.PutUint32(lengthBuf[:], uint32(len(data)))

		if _, err := w.file.Write(lengthBuf[:]); err != nil {
			return err
		}

		if _, err := w.file.Write(data); err != nil {
			return err
		}
		w.offset += 4 + int64(len(data))
	}

	if err := w.file.Sync(); err != nil {
		return err
	}

	// reset buffer
	w.buffer = w.buffer[:0]
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.flushUnlocked(); err != nil {
		return err
	}
	return w.file.Close()
}
