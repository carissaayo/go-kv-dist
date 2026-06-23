package node

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

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

// Snapshot is implemented in Phase 5.
func (s *Storage) Snapshot() (raftpb.Snapshot, error) {
	return raftpb.Snapshot{}, raft.ErrUnavailable
}

// Returns log entries in the range [lo, hi], etcd/raft calls this when replicating entries to followers.
func (s *Storage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	if lo < s.firstIndex {
		return nil, raft.ErrCompacted
	}
	if hi-1 > s.lastIndex {
		return nil, raft.ErrUnavailable
	}

	// Open a fresh read handle — same pattern as RaftLog.Scan and WAL.Replay.
	f, err := os.Open(s.meta.path)
	if err != nil {
		return nil, fmt.Errorf("storage: open raft.log for read: %w", err)
	}
	defer f.Close()

	var (
		entries   []raftpb.Entry
		totalSize uint64
	)

	for idx := lo; idx < hi; idx++ {
		indexEntry, ok := s.index[idx]
		if !ok {
			return nil, raft.ErrUnavailable
		}

		// Seek directly to this entry's byte offset — no scanning from the start
		if _, err := f.Seek(indexEntry.offset, io.SeekStart); err != nil {
			return nil, fmt.Errorf("storage: seek to offset %d: %w", indexEntry.offset, err)
		}

		// Read the raw record bytes from raft.log
		payload, err := readRecord(f)
		if err != nil {
			return nil, fmt.Errorf("storage: read record at offset %d: %w", indexEntry.offset, err)
		}

		// Decode the raw bytes back into a raftpb.Entry
		var entry raftpb.Entry
		if err := proto.Unmarshal(payload, &entry); err != nil {
			return nil, fmt.Errorf("storage: unmarshal entry %d: %w", idx, err)
		}

		// Enforce maxSize — always include the first entry, stop before
		// adding one that would push us over the limit
		entrySize := uint64(proto.Size(&entry))
		if len(entries) > 0 && totalSize+entrySize > maxSize {
			break
		}

		entries = append(entries, entry)
		totalSize += entrySize
	}

	return entries, nil
}

// This reads one length-prefixed record from f at its current position. It mirrors the record layout written by RaftLog.Append and returns the raw payload bytes.
func readRecord(f *os.File) ([]byte, error) {
	// Length prefix
	var lenBuf [4]byte
	if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
		return nil, fmt.Errorf("read length prefix: %w", err)
	}
	const maxLen = 16 << 20
	payloadLen := binary.BigEndian.Uint32(lenBuf[:])
	if payloadLen == 0 || payloadLen > maxLen {
		return nil, errors.New("record: invalid payload length")
	}

	// Payload
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(f, payload); err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}

	// CRC
	var crcBuf [4]byte
	if _, err := io.ReadFull(f, crcBuf[:]); err != nil {
		return nil, fmt.Errorf("read crc: %w", err)
	}

	h := crc32.NewIEEE()
	h.Write(lenBuf[:])
	h.Write(payload)
	if h.Sum32() != binary.BigEndian.Uint32(crcBuf[:]) {
		return nil, errors.New("record: checksum mismatch")
	}

	return payload, nil
}
