package node

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/carissaayo/go-durable-kv/pkg/engine"
	"github.com/carissaayo/go-kv-dist/internal/kv"
)

const snapMetaFile = "raft_snap_meta"

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

// Snapshot builds a raft snapshot from the current applied KV state.
func (s *Storage) Snapshot() (raftpb.Snapshot, error) {
	if s.lastIndex == 0 {
		return raftpb.Snapshot{}, raft.ErrUnavailable
	}

	applied := s.lastAppliedIndex()
	if applied == 0 {
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

func (s *Storage) loadSnapMeta() error {
	path := filepath.Join(s.dataDir, snapMetaFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("storage: read snap meta: %w", err)
	}
	if len(data) != 24 {
		return fmt.Errorf("storage: snap meta: want 24 bytes, got %d", len(data))
	}

	s.snapIndex = binary.BigEndian.Uint64(data[0:8])
	s.snapTerm = binary.BigEndian.Uint64(data[8:16])
	s.firstIndex = binary.BigEndian.Uint64(data[16:24])
	if s.firstIndex == 0 {
		s.firstIndex = 1
	}
	return nil
}

func (s *Storage) saveSnapMeta() error {
	path := filepath.Join(s.dataDir, snapMetaFile)
	tmp := path + ".tmp"

	buf := make([]byte, 24)
	binary.BigEndian.PutUint64(buf[0:8], s.snapIndex)
	binary.BigEndian.PutUint64(buf[8:16], s.snapTerm)
	binary.BigEndian.PutUint64(buf[16:24], s.firstIndex)

	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return fmt.Errorf("storage: write snap meta tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("storage: rename snap meta: %w", err)
	}
	return nil
}

// ApplySnapshot installs a raft snapshot from a peer (or local compact prep).
func (s *Storage) ApplySnapshot(snap raftpb.Snapshot) error {
	idx := snap.Metadata.Index
	term := snap.Metadata.Term

	hs, _, err := s.meta.Load()
	if err != nil {
		return fmt.Errorf("storage: load meta for snapshot: %w", err)
	}
	if err := s.meta.Save(hs, snap.Metadata.ConfState); err != nil {
		return fmt.Errorf("storage: save conf state from snapshot: %w", err)
	}

	if err := s.log.Truncate(0); err != nil {
		return fmt.Errorf("storage: truncate log for snapshot: %w", err)
	}

	s.index = make(map[uint64]logIndexEntry)
	s.firstIndex = idx + 1
	s.snapIndex = idx
	s.snapTerm = term
	if s.lastIndex < idx {
		s.lastIndex = idx
	}

	return s.saveSnapMeta()
}

// Compact removes log entries through compactTo (inclusive) from disk and memory.
func (s *Storage) Compact(compactTo uint64) error {
	if compactTo < s.firstIndex {
		return nil
	}

	ent, ok := s.index[compactTo]
	if !ok {
		return fmt.Errorf("storage: compact: missing index %d", compactTo)
	}

	keepFrom := compactTo + 1
	keepEnt, hasKeep := s.index[keepFrom]

	var dropPrefix int64
	if hasKeep {
		dropPrefix = keepEnt.offset
	} else {
		_, nextOffset, err := s.log.ReadAt(ent.offset)
		if err != nil {
			return fmt.Errorf("storage: compact read offset: %w", err)
		}
		dropPrefix = nextOffset
	}

	if err := s.log.DropPrefix(dropPrefix); err != nil {
		return fmt.Errorf("storage: drop prefix: %w", err)
	}

	if hasKeep {
		delta := keepEnt.offset
		for idx, ie := range s.index {
			if idx >= keepFrom {
				ie.offset -= delta
				s.index[idx] = ie
			}
		}
	}

	for i := s.firstIndex; i <= compactTo; i++ {
		delete(s.index, i)
	}

	s.firstIndex = compactTo + 1
	s.snapIndex = compactTo
	s.snapTerm = ent.term

	return s.saveSnapMeta()
}
