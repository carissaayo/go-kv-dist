package node

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestCluster_LeaderCrash(t *testing.T) {
	c := newTestCluster(t, 1, 2, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := c.waitForLeader(ctx)

	if err := leader.Set(ctx, "before-crash", []byte("v1")); err != nil {
		t.Fatalf("Set before crash: %v", err)
	}
	c.waitUntilGetAll(ctx, "before-crash", "v1")

	crashedID := leader.ID()
	t.Logf("crashing leader node %d", crashedID)
	c.stopMember(crashedID)

	newLeader := c.waitForLeaderExcluding(ctx, crashedID)
	t.Logf("new leader is node %d", newLeader.ID())

	if err := newLeader.Set(ctx, "after-crash", []byte("v2")); err != nil {
		t.Fatalf("Set after crash: %v", err)
	}

	c.waitUntilGetAll(ctx, "before-crash", "v1")
	c.waitUntilGetAll(ctx, "after-crash", "v2")
}

func TestCluster_FollowerRejoin(t *testing.T) {
	c := newTestCluster(t, 1, 2, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := c.waitForLeader(ctx)

	if err := leader.Set(ctx, "key1", []byte("v1")); err != nil {
		t.Fatalf("Set key1: %v", err)
	}
	c.waitUntilGetAll(ctx, "key1", "v1")

	follower := c.findFollower()
	if follower == nil {
		t.Fatal("no follower found")
	}
	followerID := follower.ID()
	t.Logf("stopping follower node %d", followerID)
	c.stopMember(followerID)

	if err := leader.Set(ctx, "key2", []byte("v2")); err != nil {
		t.Fatalf("Set key2 while follower down: %v", err)
	}
	if err := leader.Set(ctx, "key3", []byte("v3")); err != nil {
		t.Fatalf("Set key3 while follower down: %v", err)
	}

	for _, key := range []string{"key1", "key2", "key3"} {
		c.waitUntilGetAll(ctx, key, fmt.Sprintf("v%s", key[3:]))
	}

	t.Logf("restarting follower node %d", followerID)
	c.restartMember(followerID)

	rejoined := c.node(followerID)
	for _, tc := range []struct {
		key, want string
	}{
		{"key1", "v1"},
		{"key2", "v2"},
		{"key3", "v3"},
	} {
		waitUntilGet(ctx, t, rejoined, tc.key, tc.want)
	}
}

func TestCluster_LeaderRestart(t *testing.T) {
	c := newTestCluster(t, 1, 2, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := c.waitForLeader(ctx)
	leaderID := leader.ID()

	const nKeys = 20
	for i := 0; i < nKeys; i++ {
		key := fmt.Sprintf("key-%02d", i)
		val := fmt.Sprintf("val-%02d", i)
		if err := leader.Set(ctx, key, []byte(val)); err != nil {
			t.Fatalf("Set %q: %v", key, err)
		}
	}

	for i := 0; i < nKeys; i++ {
		key := fmt.Sprintf("key-%02d", i)
		want := fmt.Sprintf("val-%02d", i)
		c.waitUntilGetAll(ctx, key, want)
	}

	t.Logf("restarting leader node %d", leaderID)
	c.restartMember(leaderID)

	restarted := c.node(leaderID)
	if err := waitForLeader(ctx, restarted); err != nil {
		t.Fatalf("restarted leader rejoined: %v", err)
	}

	for i := 0; i < nKeys; i++ {
		key := fmt.Sprintf("key-%02d", i)
		want := fmt.Sprintf("val-%02d", i)
		waitUntilGet(ctx, t, restarted, key, want)
	}

	// Remaining nodes should still see all keys.
	for _, n := range c.runningNodes() {
		if n.ID() == leaderID {
			continue
		}
		for i := 0; i < nKeys; i++ {
			key := fmt.Sprintf("key-%02d", i)
			want := fmt.Sprintf("val-%02d", i)
			waitUntilGet(ctx, t, n, key, want)
		}
	}
}

func TestCluster_WritesContinueWithOneFollowerDown(t *testing.T) {
	c := newTestCluster(t, 1, 2, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	leader := c.waitForLeader(ctx)

	follower := c.findFollower()
	if follower == nil {
		t.Fatal("no follower found")
	}
	c.stopMember(follower.ID())

	// Majority (2/3) should still accept writes.
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("quorum-%d", i)
		if err := leader.Set(ctx, key, []byte("ok")); err != nil {
			t.Fatalf("Set %q with follower down: %v", key, err)
		}
	}

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("quorum-%d", i)
		c.waitUntilGetAll(ctx, key, "ok")
	}
}
