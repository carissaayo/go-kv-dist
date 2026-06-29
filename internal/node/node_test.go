package node

import (
	"context"
	"testing"
	"time"
)

func TestNode_ProposeAndRestart(t *testing.T) {
	t.Parallel()

	const id uint64 = 1
	dir := t.TempDir()

	n, err := NewNode(dir, id, Options{})
	if err != nil {
		t.Fatalf("NewNode() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := waitUntilLeader(ctx, n, id); err != nil {
		t.Fatalf("waitUntilLeader() error = %v", err)
	}

	lastBefore, err := n.Storage().LastIndex()
	if err != nil {
		t.Fatalf("LastIndex() error = %v", err)
	}

	if err := n.Propose(ctx, []byte("noop")); err != nil {
		t.Fatalf("Propose() error = %v", err)
	}

	if err := waitUntilLogGrows(ctx, n, lastBefore); err != nil {
		t.Fatalf("waitUntilLogGrows() error = %v", err)
	}

	last, err := n.Storage().LastIndex()
	if err != nil {
		t.Fatalf("LastIndex() error = %v", err)
	}
	if last == 0 {
		t.Fatalf("LastIndex() = 0, want > 0")
	}

	n.Stop()

	n2, err := NewNode(dir, id, Options{})
	if err != nil {
		t.Fatalf("NewNode() restart error = %v", err)
	}
	defer n2.Stop()

	last2, err := n2.Storage().LastIndex()
	if err != nil {
		t.Fatalf("LastIndex() after restart error = %v", err)
	}
	if last2 != last {
		t.Fatalf("LastIndex() after restart = %d, want %d", last2, last)
	}
}

func waitUntilLeader(ctx context.Context, n *Node, id uint64) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if n.Status().Lead == id {
				return nil
			}
		}
	}
}

func waitUntilLogGrows(ctx context.Context, n *Node, prev uint64) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			last, err := n.Storage().LastIndex()
			if err != nil {
				return err
			}
			if last > prev {
				return nil
			}
		}
	}
}
