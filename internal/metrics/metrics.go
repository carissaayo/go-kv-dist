package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

const labelNodeID = "node_id"

var (
	LeaderChanges = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "raft_leader_changes_total",
			Help: "Number of leader elections observed by this node.",
		},
		[]string{labelNodeID},
	)

	CommitIndex = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "raft_commit_index",
			Help: "Current committed raft log index on this node.",
		},
		[]string{labelNodeID},
	)

	ApplyLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "raft_apply_lag",
			Help: "Difference between commit index and last applied index.",
		},
		[]string{labelNodeID},
	)

	ProposalsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "raft_proposals_total",
			Help: "Total write proposals attempted on this node.",
		},
		[]string{labelNodeID},
	)

	ProposalsFailed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "raft_proposals_failed_total",
			Help: "Write proposals that failed (not leader, timeout, etc.).",
		},
		[]string{labelNodeID},
	)

	SnapshotDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kv_snapshot_duration_seconds",
			Help:    "Time taken to create or install a KV snapshot.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5},
		},
		[]string{labelNodeID},
	)
)

func init() {
	prometheus.MustRegister(
		LeaderChanges,
		CommitIndex,
		ApplyLag,
		ProposalsTotal,
		ProposalsFailed,
		SnapshotDuration,
	)
}

func NodeLabel(id uint64) string {
	return strconv.FormatUint(id, 10)
}
