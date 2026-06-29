package node

import (
	"fmt"
	"path/filepath"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/carissaayo/go-durable-kv/pkg/engine"
	"github.com/carissaayo/go-durable-kv/pkg/raftlog"
	"github.com/gogo/protobuf/proto"
)

// Compile-time check that Storage implements raft.Storage.
var _ raft.Storage = (*Storage)(nil)

type logIndexEntry struct {
	offset int64  // byte offset in raft.log
	term   uint64 // from entry.Term
}

type Storage struct {
	dataDir      string
	nodeID       uint64
	log          *raftlog.RaftLog
	meta         *RaftMeta
	eng          *engine.Engine
	appliedIndex uint64
	index        map[uint64]logIndexEntry // raft Index → offset + term
	firstIndex   uint64
	lastIndex    uint64
	snapIndex    uint64 // metadata.Index of last installed snapshot
	snapTerm     uint64 // metadata.Term of last installed snapshot
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

	if err := s.loadSnapMeta(); err != nil {
		_ = meta.Close()
		_ = raftLog.Close()
		return nil, err
	}

	if err := raftLog.Scan(func(offset int64, payload []byte) error {
		var entry raftpb.Entry
		if err := entry.Unmarshal(payload); err != nil {
			return fmt.Errorf("storage: unmarshal entry at offset %d: %w", offset, err)
		}

		if entry.Index < s.firstIndex {
			return nil
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

func (s *Storage) Close() error {
	if s == nil {
		return nil
	}
	var err error
	if s.meta != nil {
		if closeErr := s.meta.Close(); closeErr != nil {
			err = closeErr
		}
	}
	if s.log != nil {
		if closeErr := s.log.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

// Called once by etcd/raft on startup to return the HardState and ConfState last persisted to raft_meta.
func (s *Storage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	hs, cs, err := s.meta.Load()
	if err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, fmt.Errorf("storage: load meta: %w", err)
	}
	return hs, cs, nil
}

func (s *Storage) LastIndex() (uint64, error) {
	return s.lastIndex, nil
}

func (s *Storage) FirstIndex() (uint64, error) {
	return s.firstIndex, nil
}

// Term returns the election term of the log entry at index i.
func (s *Storage) Term(i uint64) (uint64, error) {
	if i < s.firstIndex {
		if i+1 == s.firstIndex {
			if s.firstIndex > 1 {
				return s.snapTerm, nil
			}
			return 0, nil
		}
		return 0, raft.ErrCompacted
	}

	if i > s.lastIndex {
		return 0, raft.ErrUnavailable
	}

	entry, ok := s.index[i]
	if !ok {
		return 0, raft.ErrUnavailable
	}

	return entry.term, nil
}

// Entries returns log entries in the range [lo, hi).
func (s *Storage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	if lo >= hi {
		return nil, nil
	}
	if lo < s.firstIndex {
		return nil, raft.ErrCompacted
	}
	if hi-1 > s.lastIndex {
		return nil, raft.ErrUnavailable
	}

	var (
		entries   []raftpb.Entry
		totalSize uint64
	)

	for idx := lo; idx < hi; idx++ {
		indexEntry, ok := s.index[idx]
		if !ok {
			return nil, raft.ErrUnavailable
		}

		payload, _, err := s.log.ReadAt(indexEntry.offset)
		if err != nil {
			return nil, fmt.Errorf("storage: read record at offset %d: %w", indexEntry.offset, err)
		}

		var entry raftpb.Entry
		if err := entry.Unmarshal(payload); err != nil {
			return nil, fmt.Errorf("storage: unmarshal entry %d: %w", idx, err)
		}

		entrySize := uint64(proto.Size(&entry))
		if len(entries) > 0 && maxSize > 0 && totalSize+entrySize > maxSize {
			break
		}

		entries = append(entries, entry)
		totalSize += entrySize
	}

	return entries, nil
}

// Append persists new raft entries from Ready().Entries and updates the in-memory index.
func (s *Storage) Append(entries []raftpb.Entry) error {
	for _, ent := range entries {
		payload, err := ent.Marshal()
		if err != nil {
			return fmt.Errorf("storage: marshal entry %d: %w", ent.Index, err)
		}

		offset, err := s.log.Append(payload)
		if err != nil {
			return fmt.Errorf("storage: append entry %d: %w", ent.Index, err)
		}

		s.index[ent.Index] = logIndexEntry{
			offset: offset,
			term:   ent.Term,
		}

		if ent.Index > s.lastIndex {
			s.lastIndex = ent.Index
		}
	}
	return nil
}

func (s *Storage) SaveHardState(hs raftpb.HardState) error {
	_, cs, err := s.meta.Load()
	if err != nil {
		return fmt.Errorf("storage: load conf state: %w", err)
	}

	if err := s.meta.Save(hs, cs); err != nil {
		return fmt.Errorf("storage: save meta: %w", err)
	}

	return nil
}

func (s *Storage) SaveConfState(cs raftpb.ConfState) error {
	hs, _, err := s.meta.Load()
	if err != nil {
		return fmt.Errorf("storage: load hard state: %w", err)
	}

	if err := s.meta.Save(hs, cs); err != nil {
		return fmt.Errorf("storage: save conf state: %w", err)
	}

	return nil
}
