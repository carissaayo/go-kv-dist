package node

import (
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
