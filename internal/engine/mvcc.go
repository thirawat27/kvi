package engine

import (
	"sync"
	"time"

	"github.com/thirawat27/kvi/pkg/types"
)

type VersionedRecord struct {
	TxID      uint64
	Timestamp int64
	Record    *types.Record
}

type MVCCManager struct {
	versions map[string][]*VersionedRecord
	mu       sync.RWMutex
	lastTxID uint64
}

func NewMVCCManager() *MVCCManager {
	return &MVCCManager{
		versions: make(map[string][]*VersionedRecord),
	}
}

func (m *MVCCManager) Put(key string, record *types.Record) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastTxID++
	vr := &VersionedRecord{
		TxID:      m.lastTxID,
		Timestamp: time.Now().UnixNano(),
		Record:    record,
	}

	m.versions[key] = append(m.versions[key], vr)
	return m.lastTxID
}

func (m *MVCCManager) Get(key string) (*types.Record, uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	vrs, ok := m.versions[key]
	if !ok || len(vrs) == 0 {
		return nil, 0
	}

	last := vrs[len(vrs)-1]
	return last.Record, last.TxID
}

// GetAsOf supports time-travel queries
func (m *MVCCManager) GetAsOf(key string, txID uint64) *types.Record {
	m.mu.RLock()
	defer m.mu.RUnlock()

	vrs, ok := m.versions[key]
	if !ok {
		return nil
	}

	// Binary search for version lookup
	left, right := 0, len(vrs)-1
	var result *VersionedRecord

	for left <= right {
		mid := left + (right-left)/2
		if vrs[mid].TxID <= txID {
			result = vrs[mid]
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	if result != nil {
		return result.Record
	}
	return nil
}

func (m *MVCCManager) GC(olderThanTxID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, vrs := range m.versions {
		var filtered []*VersionedRecord
		// Keep at least the latest or versions >= olderThanTxID
		for i, vr := range vrs {
			if vr.TxID >= olderThanTxID || i == len(vrs)-1 {
				filtered = append(filtered, vr)
			}
		}
		m.versions[key] = filtered
	}
}
