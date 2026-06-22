package node

import (
	"fmt"
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

}

func (r *RaftMeta) Save(hs raftpb.HardState, cs raftpb.ConfState) error {

}

func (r *RaftMeta) Close() error {

}
