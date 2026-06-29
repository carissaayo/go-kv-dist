package node

import (
	"log/slog"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/carissaayo/go-kv-dist/internal/metrics"
)

func (n *Node) observeReady(rd raft.Ready) {
	st := n.Status()
	n.observeLeader(st)

	commit := st.Commit
	if !raft.IsEmptyHardState(rd.HardState) && rd.HardState.Commit > 0 {
		commit = rd.HardState.Commit
	}

	id := metrics.NodeLabel(n.id)
	metrics.CommitIndex.WithLabelValues(id).Set(float64(commit))

	applied := n.lastApplied.Load()
	lag := uint64(0)
	if commit > applied {
		lag = commit - applied
	}
	metrics.ApplyLag.WithLabelValues(id).Set(float64(lag))
}

func (n *Node) observeLeader(st raft.Status) {
	newLead := st.Lead
	oldLead := n.lastLeader.Load()
	if newLead == oldLead {
		return
	}
	n.lastLeader.Store(newLead)

	if newLead == 0 {
		slog.Info("raft leader lost",
			"node_id", n.id,
			"previous_leader", oldLead,
			"term", st.Term,
		)
		return
	}

	if oldLead != 0 && oldLead != newLead {
		metrics.LeaderChanges.WithLabelValues(metrics.NodeLabel(n.id)).Inc()
	}

	slog.Info("raft leader changed",
		"node_id", n.id,
		"previous_leader", oldLead,
		"leader", newLead,
		"term", st.Term,
		"self_is_leader", newLead == n.id,
	)
}

func (n *Node) logEntryApplied(ent raftpb.Entry) {
	switch ent.Type {
	case raftpb.EntryConfChange:
		slog.Info("raft conf change applied",
			"node_id", n.id,
			"index", ent.Index,
			"term", ent.Term,
		)
	case raftpb.EntryNormal:
		if len(ent.Data) > 0 {
			slog.Debug("raft entry applied",
				"node_id", n.id,
				"index", ent.Index,
				"term", ent.Term,
			)
		}
	}
}

func (n *Node) observeSnapshot(op string, start time.Time) {
	elapsed := time.Since(start).Seconds()
	metrics.SnapshotDuration.WithLabelValues(metrics.NodeLabel(n.id)).Observe(elapsed)
	slog.Info("raft snapshot",
		"node_id", n.id,
		"op", op,
		"duration_seconds", elapsed,
	)
}

func (n *Node) recordProposalFailed(reason string, err error) {
	id := metrics.NodeLabel(n.id)
	metrics.ProposalsTotal.WithLabelValues(id).Inc()
	metrics.ProposalsFailed.WithLabelValues(id).Inc()
	attrs := []any{
		"node_id", n.id,
		"reason", reason,
	}
	if err != nil {
		attrs = append(attrs, "error", err)
	}
	slog.Warn("proposal failed", attrs...)
}

func (n *Node) recordProposalOK() {
	id := metrics.NodeLabel(n.id)
	metrics.ProposalsTotal.WithLabelValues(id).Inc()
	slog.Debug("proposal committed", "node_id", n.id)
}
