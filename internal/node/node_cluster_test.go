package node

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func TestCluster_SetReplicates(t *testing.T) {
	const (
		id1 uint64 = 1
		id2 uint64 = 2
		id3 uint64 = 3
	)

	addr1 := freeTCPAddr(t)
	addr2 := freeTCPAddr(t)
	addr3 := freeTCPAddr(t)

	peerAddrs := map[uint64]string{
		id1: addr1,
		id2: addr2,
		id3: addr3,
	}

	type member struct {
		n   *Node
		srv *grpc.Server
	}
	members := []struct {
		id   uint64
		dir  string
		addr string
	}{
		{id1, t.TempDir(), addr1},
		{id2, t.TempDir(), addr2},
		{id3, t.TempDir(), addr3},
	}

	listeners := make([]net.Listener, len(members))
	for i, m := range members {
		lis, err := net.Listen("tcp", m.addr)
		if err != nil {
			t.Fatalf("listen %q: %v", m.addr, err)
		}
		listeners[i] = lis
	}

	running := make([]member, len(members))
	for i, m := range members {
		n, err := NewNode(m.dir, m.id, Options{PeerAddrs: peerAddrs})
		if err != nil {
			t.Fatalf("NewNode(%d) error = %v", m.id, err)
		}
		srv := grpc.NewServer()
		RegisterRaftTransport(srv, n)
		go func(s *grpc.Server, l net.Listener) {
			_ = s.Serve(l)
		}(srv, listeners[i])
		running[i] = member{n: n, srv: srv}
	}
	defer func() {
		for _, m := range running {
			m.srv.GracefulStop()
			m.n.Stop()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := waitForLeader(ctx, running[0].n); err != nil {
		t.Fatalf("waitForLeader: %v", err)
	}

	var leader *Node
	for _, m := range running {
		if m.n.LeaderID() == m.n.ID() {
			leader = m.n
			break
		}
	}
	if leader == nil {
		t.Fatal("no leader elected")
	}

	if err := leader.Set(ctx, "replicate", []byte("value")); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	for _, m := range running {
		waitUntilGet(ctx, t, m.n, "replicate", "value")
	}
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
