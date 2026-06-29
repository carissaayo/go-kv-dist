package node

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
)

type clusterMember struct {
	id   uint64
	dir  string
	addr string

	lis net.Listener
	n   *Node
	srv *grpc.Server
}

type testCluster struct {
	t         *testing.T
	peerAddrs map[uint64]string
	members   map[uint64]*clusterMember
}

func newTestCluster(t *testing.T, ids ...uint64) *testCluster {
	t.Helper()

	c := &testCluster{
		t:         t,
		peerAddrs: make(map[uint64]string, len(ids)),
		members:   make(map[uint64]*clusterMember, len(ids)),
	}

	for _, id := range ids {
		addr := freeTCPAddr(t)
		c.peerAddrs[id] = addr
		c.members[id] = &clusterMember{
			id:   id,
			dir:  t.TempDir(),
			addr: addr,
		}
	}

	t.Cleanup(c.stopAll)

	for id := range c.members {
		if err := c.startMember(id); err != nil {
			t.Fatalf("start member %d: %v", id, err)
		}
	}

	return c
}

func (c *testCluster) startMember(id uint64) error {
	m := c.members[id]

	lis, err := net.Listen("tcp", m.addr)
	if err != nil {
		return err
	}

	n, err := NewNode(m.dir, m.id, Options{PeerAddrs: c.peerAddrs})
	if err != nil {
		_ = lis.Close()
		return err
	}

	srv := grpc.NewServer()
	RegisterRaftTransport(srv, n)
	go func() { _ = srv.Serve(lis) }()

	m.lis = lis
	m.n = n
	m.srv = srv
	return nil
}

func (c *testCluster) stopMember(id uint64) {
	m := c.members[id]
	if m.n == nil {
		return
	}
	m.srv.GracefulStop()
	m.n.Stop()
	_ = m.lis.Close()
	m.lis = nil
	m.n = nil
	m.srv = nil
}

func (c *testCluster) restartMember(id uint64) {
	c.t.Helper()
	c.stopMember(id)
	if err := c.startMember(id); err != nil {
		c.t.Fatalf("restart member %d: %v", id, err)
	}
}

func (c *testCluster) stopAll() {
	for id := range c.members {
		c.stopMember(id)
	}
}

func (c *testCluster) node(id uint64) *Node {
	return c.members[id].n
}

func (c *testCluster) runningNodes() []*Node {
	var out []*Node
	for _, m := range c.members {
		if m.n != nil {
			out = append(out, m.n)
		}
	}
	return out
}

func (c *testCluster) findLeader() *Node {
	for _, m := range c.members {
		if m.n == nil {
			continue
		}
		if m.n.LeaderID() == m.n.ID() {
			return m.n
		}
	}
	return nil
}

func (c *testCluster) findFollower() *Node {
	lead := uint64(0)
	for _, m := range c.members {
		if m.n != nil {
			lead = m.n.LeaderID()
			break
		}
	}
	for _, m := range c.members {
		if m.n != nil && m.n.ID() != lead {
			return m.n
		}
	}
	return nil
}

func (c *testCluster) waitForLeader(ctx context.Context) *Node {
	c.t.Helper()
	if err := waitForLeader(ctx, c.runningNodes()[0]); err != nil {
		c.t.Fatalf("waitForLeader: %v", err)
	}
	leader := c.findLeader()
	if leader == nil {
		c.t.Fatal("no leader elected")
	}
	return leader
}

func (c *testCluster) waitForLeaderExcluding(ctx context.Context, exclude uint64) *Node {
	c.t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.t.Fatalf("waitForLeaderExcluding(%d): %v", exclude, ctx.Err())
		case <-ticker.C:
			leader := c.findLeader()
			if leader != nil && leader.ID() != exclude {
				return leader
			}
		}
	}
}

func (c *testCluster) waitUntilGetAll(ctx context.Context, key, want string) {
	c.t.Helper()
	for _, n := range c.runningNodes() {
		waitUntilGet(ctx, c.t, n, key, want)
	}
}
