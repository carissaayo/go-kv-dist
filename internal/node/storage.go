package node

import (
	"fmt"
	"path/filepath"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/carissaayo/go-durable-kv/pkg/engine"
	"github.com/carissaayo/go-durable-kv/pkg/raftlog"
	"github.com/carissaayo/go-kv-dist/internal/kv"
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

func (s *Storage) Snapshot() (raftpb.Snapshot, error) {
	if s.lastIndex == 0 {
		return raftpb.Snapshot{}, raft.ErrUnavailable
	}

	applied := s.lastAppliedIndex()
	if applied == 0 {
		// No KV applied yet; conf-change-only log — not ready to snap KV state.
		return raftpb.Snapshot{}, raft.ErrUnavailable
	}

	data, err := s.engineSnapshotData()
	if err != nil {
		return raftpb.Snapshot{}, err
	}
	payload, err := kv.EncodeState(data)
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	entry, ok := s.index[applied]
	if !ok {
		return raftpb.Snapshot{}, fmt.Errorf("storage: no term for applied index %d", applied)
	}

	_, cs, err := s.meta.Load()
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	return raftpb.Snapshot{
		Data: payload,
		Metadata: raftpb.SnapshotMetadata{
			Index:     applied,
			Term:      entry.term,
			ConfState: cs,
		},
	}, nil
}

// Returns log entries in the range [lo, hi], etcd/raft calls this when replicating entries to followers.
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

		// Decode the raw bytes back into a raftpb.Entry
		var entry raftpb.Entry
		if err := entry.Unmarshal(payload); err != nil {
			return nil, fmt.Errorf("storage: unmarshal entry %d: %w", idx, err)
		}

		// Enforce maxSize — always include the first entry, stop before
		// adding one that would push us over the limit
		entrySize := uint64(proto.Size(&entry))
		if len(entries) > 0 && maxSize > 0 && totalSize+entrySize > maxSize {
			break
		}

		entries = append(entries, entry)
		totalSize += entrySize
	}

	return entries, nil
}

// Persists new raft entries from Ready().Entries and updates the in-memory index.
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

// Persists HardState from Ready() while ConfState is reloaded from raft_meta.
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

// Persists an updated cluster configuration.
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

func (s *Storage) SetEngine(eng *engine.Engine) {
	s.eng = eng
}

// SetAppliedIndex records the highest log index applied to the KV engine.
func (s *Storage) SetAppliedIndex(idx uint64) {
	s.appliedIndex = idx
}
func (s *Storage) engineSnapshotData() (map[string][]byte, error) {
	if s.eng == nil {
		return nil, fmt.Errorf("storage: engine not set")
	}
	return engine.SnapshotData(s.eng)
}
func (s *Storage) lastAppliedIndex() uint64 {
	return s.appliedIndex
}
