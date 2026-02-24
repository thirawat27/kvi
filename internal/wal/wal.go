package wal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/thirawat27/kvi/pkg/types"
)

// WAL (Write-Ahead Logging) implements durability for the database
type WAL struct {
	file   *os.File
	path   string
	mu     sync.Mutex
	offset int64

	// Buffering
	buffer []LogEntry
	bufMu  sync.Mutex

	// Stats
	writeCount int64
	flushCount int64
}

// LogEntry represents a single log entry
type LogEntry struct {
	LSN       uint64          `json:"lsn"`       // Log Sequence Number
	Timestamp int64           `json:"timestamp"` // Unix nano timestamp
	Op        types.Operation `json:"op"`        // PUT, DELETE, BATCH
	Key       string          `json:"key"`
	Record    *types.Record   `json:"record"`
	Checksum  uint32          `json:"checksum"` // CRC32 of entire entry
}

// OpenWAL opens or creates a WAL file
func OpenWAL(path string) (*WAL, error) {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	// Get file size
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to stat WAL file: %w", err)
	}

	wal := &WAL{
		file:   f,
		path:   path,
		offset: stat.Size(),
		buffer: make([]LogEntry, 0, 1000),
	}

	return wal, nil
}

// Append adds a new entry to the WAL buffer
func (w *WAL) Append(entry *LogEntry) error {
	entry.Timestamp = time.Now().UnixNano()
	entry.LSN = uint64(entry.Timestamp)

	// Calculate checksum before writing
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal log entry: %w", err)
	}
	entry.Checksum = crc32.ChecksumIEEE(data)

	w.bufMu.Lock()
	w.buffer = append(w.buffer, *entry)
	shouldFlush := len(w.buffer) >= 1000 // Flush every 1000 entries
	w.bufMu.Unlock()

	w.writeCount++

	if shouldFlush {
		return w.Flush()
	}

	return nil
}

// Flush writes buffered entries to disk
func (w *WAL) Flush() error {
	w.bufMu.Lock()
	entries := w.buffer
	w.buffer = make([]LogEntry, 0, 1000)
	w.bufMu.Unlock()

	if len(entries) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("failed to marshal entry: %w", err)
		}

		// Write length prefix (4 bytes) + data
		length := uint32(len(data))
		if err := binary.Write(w.file, binary.LittleEndian, length); err != nil {
			return fmt.Errorf("failed to write length: %w", err)
		}
		if _, err := w.file.Write(data); err != nil {
			return fmt.Errorf("failed to write data: %w", err)
		}
		w.offset += int64(4 + len(data))
	}

	// fsync for durability
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	w.flushCount++

	return nil
}

// ReadAll reads all entries from the WAL
func (w *WAL) ReadAll() ([]*LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Flush any buffered entries first
	if err := w.Flush(); err != nil {
		return nil, err
	}

	// Seek to beginning
	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek WAL: %w", err)
	}

	var entries []*LogEntry

	for {
		// Read length prefix
		var length uint32
		err := binary.Read(w.file, binary.LittleEndian, &length)
		if err != nil {
			// EOF is expected
			break
		}

		// Read data
		data := make([]byte, length)
		if _, err := w.file.Read(data); err != nil {
			break
		}

		// Unmarshal entry
		var entry LogEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			// Skip corrupted entries
			continue
		}

		// Verify checksum
		storedChecksum := entry.Checksum
		entry.Checksum = 0
		checksumData, _ := json.Marshal(entry)
		if crc32.ChecksumIEEE(checksumData) != storedChecksum {
			// Skip corrupted entries
			continue
		}
		entry.Checksum = storedChecksum

		entries = append(entries, &entry)
	}

	// Seek back to end
	if _, err := w.file.Seek(0, 2); err != nil {
		return nil, fmt.Errorf("failed to seek WAL to end: %w", err)
	}

	return entries, nil
}

// Close closes the WAL file
func (w *WAL) Close() error {
	// Flush remaining entries
	if err := w.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// Size returns the current size of the WAL file
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offset
}

// Truncate truncates the WAL file (used after checkpoint)
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Close(); err != nil {
		return err
	}

	// Create new empty file
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	w.file = f
	w.offset = 0

	return nil
}

// Stats returns WAL statistics
func (w *WAL) Stats() WALStats {
	return WALStats{
		WriteCount: w.writeCount,
		FlushCount: w.flushCount,
		Size:       w.offset,
		Path:       w.path,
	}
}

// WALStats contains WAL statistics
type WALStats struct {
	WriteCount int64
	FlushCount int64
	Size       int64
	Path       string
}

// NewLogEntry creates a new log entry
func NewLogEntry(op types.Operation, key string, record *types.Record) *LogEntry {
	return &LogEntry{
		Op:     op,
		Key:    key,
		Record: record,
	}
}
