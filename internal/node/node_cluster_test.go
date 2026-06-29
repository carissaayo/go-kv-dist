package node

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestCluster_SetReplicates(t *testing.T) {
	c := newTestCluster(t, 1, 2, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	leader := c.waitForLeader(ctx)

	if err := leader.Set(ctx, "replicate", []byte("value")); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	c.waitUntilGetAll(ctx, "replicate", "value")
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}

func waitForLeader(ctx context.Context, n *Node) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if n.LeaderID() != 0 {
				return nil
			}
		}
	}
}

func waitUntilGet(ctx context.Context, t *testing.T, n *Node, key, want string) {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			val, found, _ := n.Get(key)
			t.Fatalf("timeout node %d Get(%q) = (%q, %v), want %q", n.id, key, val, found, want)
		case <-ticker.C:
			val, found, err := n.Get(key)
			if err != nil {
				t.Fatalf("node %d Get() error = %v", n.id, err)
			}
			if found && string(val) == want {
				return
			}
		}
	}
}
