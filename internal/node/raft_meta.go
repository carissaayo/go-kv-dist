package node

import (
	"os"

	"go.etcd.io/raft/v3/raftpb"
)

type RaftMeta struct {
	file   *os.File
	path   string
	nodeID uint64
}

func OpenRaftMeta(path string, nodeID uint64) (*RaftMeta, error) {

}

func (r *RaftMeta) Load() (raftpb.HardState, raftpb.ConfState, error) {

}

func (r *RaftMeta) Save(hs raftpb.HardState, cs raftpb.ConfState) error {

}

func (r *RaftMeta) Close() error {

}
