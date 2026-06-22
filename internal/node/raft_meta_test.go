package node

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

func TestOpenRaftMeta_FreshNodeWritesDefaults(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "raft_meta")
	const nodeID uint64 = 42

	meta, err := OpenRaftMeta(path, nodeID)
	if err != nil {
		t.Fatalf("OpenRaftMeta() error = %v", err)
	}
	defer func() { _ = meta.Close() }()

	hs, cs, err := meta.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if hs.Term != 0 || hs.Vote != 0 || hs.Commit != 0 {
		t.Fatalf("HardState = %+v, want term/vote/commit 0", hs)
	}
	if !reflect.DeepEqual(cs.Voters, []uint64{nodeID}) {
		t.Fatalf("ConfState.Voters = %v, want [%d]", cs.Voters, nodeID)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("raft_meta file missing: %v", err)
	}
}

func TestSaveLoad_RoundTrip(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "raft_meta")

	meta, err := OpenRaftMeta(path, 1)
	if err != nil {
		t.Fatalf("OpenRaftMeta() error = %v", err)
	}
	defer func() { _ = meta.Close() }()

	wantHS := raftpb.HardState{
		Term:   7,
		Vote:   1,
		Commit: 99,
	}
	wantCS := raftpb.ConfState{
		Voters: []uint64{1, 2, 3},
	}

	if err := meta.Save(wantHS, wantCS); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	gotHS, gotCS, err := meta.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if gotHS != wantHS {
		t.Fatalf("HardState = %+v, want %+v", gotHS, wantHS)
	}
	if !reflect.DeepEqual(gotCS.Voters, wantCS.Voters) {
		t.Fatalf("ConfState.Voters = %v, want %v", gotCS.Voters, wantCS.Voters)
	}
}

func TestReopen_PreservesState(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "raft_meta")

	meta1, err := OpenRaftMeta(path, 1)
	if err != nil {
		t.Fatalf("OpenRaftMeta #1 error = %v", err)
	}

	wantHS := raftpb.HardState{Term: 3, Vote: 1, Commit: 10}
	wantCS := raftpb.ConfState{Voters: []uint64{1}}

	if err := meta1.Save(wantHS, wantCS); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	if err := meta1.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	meta2, err := OpenRaftMeta(path, 99) // different nodeID must not rewrite existing file
	if err != nil {
		t.Fatalf("OpenRaftMeta #2 error = %v", err)
	}
	defer func() { _ = meta2.Close() }()

	gotHS, gotCS, err := meta2.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if gotHS != wantHS {
		t.Fatalf("HardState = %+v, want %+v", gotHS, wantHS)
	}
	if !reflect.DeepEqual(gotCS.Voters, wantCS.Voters) {
		t.Fatalf("ConfState.Voters = %v, want %v", gotCS.Voters, wantCS.Voters)
	}
}

func TestLoad_NotInitialized(t *testing.T) {
	t.Parallel()

	var meta *RaftMeta
	_, _, err := meta.Load()
	if err == nil {
		t.Fatal("Load() on nil RaftMeta error = nil, want error")
	}

	empty := &RaftMeta{}
	_, _, err = empty.Load()
	if err == nil {
		t.Fatal("Load() with empty path error = nil, want error")
	}
}

func TestSave_NotInitialized(t *testing.T) {
	t.Parallel()

	var meta *RaftMeta
	err := meta.Save(raftpb.HardState{}, raftpb.ConfState{})
	if err == nil {
		t.Fatal("Save() on nil RaftMeta error = nil, want error")
	}
}

func TestClose_Idempotent(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "raft_meta")
	meta, err := OpenRaftMeta(path, 1)
	if err != nil {
		t.Fatalf("OpenRaftMeta() error = %v", err)
	}

	if err := meta.Close(); err != nil {
		t.Fatalf("Close() #1 error = %v", err)
	}
	if err := meta.Close(); err != nil {
		t.Fatalf("Close() #2 error = %v", err)
	}
}

func TestLoad_TruncatedFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "raft_meta")

	meta, err := OpenRaftMeta(path, 1)
	if err != nil {
		t.Fatalf("OpenRaftMeta() error = %v", err)
	}
	if err := meta.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Only a length prefix, no payload — corrupt/truncated.
	if err := os.WriteFile(path, []byte{0, 0, 0, 5}, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	meta2, err := OpenRaftMeta(path, 1)
	if err != nil {
		t.Fatalf("OpenRaftMeta #2 error = %v", err)
	}
	defer func() { _ = meta2.Close() }()

	_, _, err = meta2.Load()
	if err == nil {
		t.Fatal("Load() on truncated file error = nil, want error")
	}
}
