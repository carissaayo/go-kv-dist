package node

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.etcd.io/raft/v3/raftpb"
)

type RaftMeta struct {
	file   *os.File
	path   string
	nodeID uint64
}

// OpenRaftMeta opens or creates raft_meta at path. On a new file it writes default HardState and a single-voter ConfStat
func OpenRaftMeta(path string, nodeID uint64) (*RaftMeta, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("raftmeta: mkdir %q: %w", dir, err)
	}

	_, err := os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("raftmeta: stat %q: %w", path, err)
	}
	freshNode := os.IsNotExist(err)

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("raftmeta: open %q: %w", path, err)
	}

	r := &RaftMeta{
		file:   f,
		path:   path,
		nodeID: nodeID,
	}

	// Fresh node — write the defaults before anything else uses this file
	if freshNode {
		hs := raftpb.HardState{
			Term:   0,
			Vote:   0,
			Commit: 0,
		}
		cs := raftpb.ConfState{
			Voters: []uint64{nodeID},
		}

		if err := r.Save(hs, cs); err != nil {
			f.Close()
			return nil, fmt.Errorf("raftmeta: write defaults: %w", err)
		}
	}
	return r, nil
}

func (r *RaftMeta) Load() (raftpb.HardState, raftpb.ConfState, error) {
	var hs raftpb.HardState
	var cs raftpb.ConfState

	if r == nil || r.path == "" {
		return hs, cs, fmt.Errorf("raftmeta: not initialized")
	}

	f, err := os.Open(r.path)
	if err != nil {
		return hs, cs, fmt.Errorf("raftmeta: open for load: %w", err)
	}
	defer f.Close()

	if err := decodeMeta(f, &hs, &cs); err != nil {
		return hs, cs, err
	}
	return hs, cs, nil
}

func (r *RaftMeta) Save(hs raftpb.HardState, cs raftpb.ConfState) error {
	if r == nil || r.path == "" {
		return fmt.Errorf("raftmeta: not initialized")
	}

	data, err := encodeMeta(hs, cs)
	if err != nil {
		return err
	}

	tmpPath := r.path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("raftmeta: create tmp: %w", err)
	}

	cleanup := func(cause error) error {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return cause
	}

	if _, err := f.Write(data); err != nil {
		return cleanup(fmt.Errorf("raftmeta: write tmp: %w", err))
	}
	if err := f.Sync(); err != nil {
		return cleanup(fmt.Errorf("raftmeta: sync tmp: %w", err))
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("raftmeta: close tmp: %w", err)
	}
	if err := os.Rename(tmpPath, r.path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("raftmeta: rename: %w", err)
	}

	//Refresh open handle so a later Save on the same RaftMeta sees the new file.
	if r.file != nil {
		_ = r.file.Close()
	}
	opened, err := os.OpenFile(r.path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("raftmeta: reopen after save: %w", err)
	}
	r.file = opened
	return nil
}

func (r *RaftMeta) Close() error {
	if r == nil || r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}

func encodeMeta(hs raftpb.HardState, cs raftpb.ConfState) ([]byte, error) {
	hsBytes, err := hs.Marshal()
	if err != nil {
		return nil, fmt.Errorf("raftmeta: marshal hard state: %w", err)
	}

	csBytes, err := cs.Marshal()
	if err != nil {
		return nil, fmt.Errorf("raftmeta: marshal conf state: %w", err)
	}

	const maxLen = 16 << 20 // 16 MiB sanity cap per blob
	if len(hsBytes) > maxLen || len(csBytes) > maxLen {
		return nil, fmt.Errorf("raftmeta: encoded state too large")
	}

	out := make([]byte, 8+len(hsBytes)+len(csBytes))
	binary.BigEndian.PutUint32(out[0:4], uint32(len(hsBytes)))
	copy(out[4:4+len(hsBytes)], hsBytes)

	off := 4 + len(hsBytes)
	binary.BigEndian.PutUint32(out[off:off+4], uint32(len(csBytes)))
	copy(out[off+4:], csBytes)

	return out, nil
}

func decodeMeta(r io.Reader, hs *raftpb.HardState, cs *raftpb.ConfState) error {
	hsBytes, err := readLenPrefixed(r, "hard state")
	if err != nil {
		return err
	}
	csBytes, err := readLenPrefixed(r, "conf state")
	if err != nil {
		return err
	}
	if err := hs.Unmarshal(hsBytes); err != nil {
		return fmt.Errorf("raftmeta: unmarshal hard state: %w", err)
	}
	if err := cs.Unmarshal(csBytes); err != nil {
		return fmt.Errorf("raftmeta: unmarshal conf state: %w", err)
	}
	return nil
}

func readLenPrefixed(r io.Reader, field string) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("raftmeta: missing %s length", field)
		}
		return nil, fmt.Errorf("raftmeta: read %s length: %w", field, err)
	}

	n := binary.BigEndian.Uint32(lenBuf[:])
	if n == 0 {
		return nil, fmt.Errorf("raftmeta: empty %s", field)
	}
	if n > 16<<20 {
		return nil, fmt.Errorf("raftmeta: %s length %d too large", field, n)
	}

	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("raftmeta: read %s: %w", field, err)
	}
	return buf, nil
}
