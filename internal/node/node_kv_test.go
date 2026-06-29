package node

import (
	"context"
	"testing"
	"time"
)

func TestNode_SetGet(t *testing.T) {
	t.Parallel()

	const id uint64 = 1
	dir := t.TempDir()

	n, err := NewNode(dir, id, Options{})
	if err != nil {
		t.Fatalf("NewNode() error = %v", err)
	}
	defer n.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := waitUntilLeader(ctx, n, id); err != nil {
		t.Fatalf("waitUntilLeader() error = %v", err)
	}

	if err := n.Set(ctx, "foo", []byte("bar")); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	val, found, err := n.Get("foo")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !found || string(val) != "bar" {
		t.Fatalf("Get() = (%q, %v), want (%q, true)", val, found, "bar")
	}

	if err := n.Delete(ctx, "foo"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	_, found, err = n.Get("foo")
	if err != nil {
		t.Fatalf("Get() after delete error = %v", err)
	}
	if found {
		t.Fatal("Get() after delete: found=true, want false")
	}
}
