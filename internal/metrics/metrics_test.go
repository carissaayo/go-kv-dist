package metrics_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/carissaayo/go-kv-dist/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestMetricsRegistered(t *testing.T) {
	metrics.LeaderChanges.WithLabelValues("1").Inc()
	metrics.CommitIndex.WithLabelValues("1").Set(42)
	metrics.ApplyLag.WithLabelValues("1").Set(0)
	metrics.ProposalsTotal.WithLabelValues("1").Inc()
	metrics.ProposalsFailed.WithLabelValues("1").Inc()
	metrics.SnapshotDuration.WithLabelValues("1").Observe(0.01)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	text := string(body)
	for _, name := range []string{
		"raft_leader_changes_total",
		"raft_commit_index",
		"raft_apply_lag",
		"raft_proposals_total",
		"raft_proposals_failed_total",
		"kv_snapshot_duration_seconds",
	} {
		if !strings.Contains(text, name) {
			t.Errorf("metrics output missing %q", name)
		}
	}
}
