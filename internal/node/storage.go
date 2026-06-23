package node

import (
	"fmt"
	"path/filepath"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/carissaayo/go-durable-kv/pkg/raftlog"
)

type logIndexEntry struct {
	offset int64  // byte offset in raft.log
	term   uint64 // from entry.Term
}

type Storage struct {
	dataDir string
	nodeID  uint64

	log  *raftlog.RaftLog
	meta *RaftMeta

	index      map[uint64]logIndexEntry // raft Index → offset + term
	firstIndex uint64                   // 1 until compaction (Phase 5)
	lastIndex  uint64
}

func OpenStorage(dataDir string, nodeID uint64) (*Storage, error) {
	raftLog, err := raftlog.OpenRaftLog(
		filepath.Join(dataDir, "raft.log"),
		raftlog.SyncAlways,
	)
	if err != nil {
		return nil, fmt.Errorf("storage: open raft log: %w", err)
	}

	meta, err := OpenRaftMeta(
		filepath.Join(dataDir, "raft_meta"),
		nodeID,
	)
	if err != nil {
		_ = raftLog.Close()
		return nil, fmt.Errorf("storage: open raft meta: %w", err)
	}

	s := &Storage{
		dataDir:    dataDir,
		nodeID:     nodeID,
		log:        raftLog,
		meta:       meta,
		index:      make(map[uint64]logIndexEntry),
		firstIndex: 1,
		lastIndex:  0,
	}

	if err := raftLog.Scan(func(offset int64, payload []byte) error {
		var entry raftpb.Entry
		if err := entry.Unmarshal(payload); err != nil {
			return fmt.Errorf("storage: unmarshal entry at offset %d: %w", offset, err)
		}

		s.index[entry.Index] = logIndexEntry{
			offset: offset,
			term:   entry.Term,
		}

		if entry.Index > s.lastIndex {
			s.lastIndex = entry.Index
		}

		return nil
	}); err != nil {
		_ = meta.Close()
		_ = raftLog.Close()
		return nil, fmt.Errorf("storage: scan raft log: %w", err)
	}

	return s, nil
}

// Called once by etcd/raft on startup to return the HardState and ConfState last persisted to raft_meta, restoring the node's consensus identity after a restart.
func (s *Storage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	hs, cs, err := s.meta.Load()
	if err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, fmt.Errorf("storage: load meta: %w", err)
	}
	return hs, cs, nil
}

// Returns the index of the most recent log entry, which is 0 on a fresh node, being used by etcd/raft to determine the next index to assign.
func (s *Storage) LastIndex() (uint64, error) {
	return s.lastIndex, nil
}

// Returns the index of the first log entry still available.
func (s *Storage) FirstIndex() (uint64, error) {
	return s.firstIndex, nil
}

// Returns the election term of the log entry at index i. Used by etcd/raft to verify log consistency between nodes — two entries match if and only if they share the same index AND term.
func (s *Storage) Term(i uint64) (uint64, error) {
	// Below the compaction boundary — we no longer have this entry
	if i < s.firstIndex {
		return 0, raft.ErrCompacted
	}

	// Above the last written entry — does not exist yet
	if i > s.lastIndex {
		return 0, raft.ErrUnavailable
	}

	entry, ok := s.index[i]
	if !ok {
		// Should never happen if firstIndex/lastIndex are consistent with index map
		return 0, raft.ErrUnavailable
	}

	return entry.term, nil
}
