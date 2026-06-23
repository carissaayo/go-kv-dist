package node

import (
	"reflect"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestOpenStorage_FreshNode(t *testing.T) {
	t.Parallel()

	const nodeID uint64 = 7
	s, err := OpenStorage(t.TempDir(), nodeID)
	if err != nil {
		t.Fatalf("OpenStorage() error = %v", err)
	}
	defer func() { _ = s.Close() }()

	last, err := s.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex() error = %v", err)
	}
	if last != 0 {
		t.Fatalf("LastIndex() = %d, want 0", last)
	}

	first, err := s.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex() error = %v", err)
	}
	if first != 1 {
		t.Fatalf("FirstIndex() = %d, want 1", first)
	}

	hs, cs, err := s.InitialState()
	if err != nil {
		t.Fatalf("InitialState() error = %v", err)
	}
	if hs.Term != 0 || hs.Vote != 0 || hs.Commit != 0 {
		t.Fatalf("HardState = %+v, want zeros", hs)
	}
	if !reflect.DeepEqual(cs.Voters, []uint64{nodeID}) {
		t.Fatalf("ConfState.Voters = %v, want [%d]", cs.Voters, nodeID)
	}
}

func TestStorage_AppendEntriesRoundTrip(t *testing.T) {
	t.Parallel()

	s, err := OpenStorage(t.TempDir(), 1)
	if err != nil {
		t.Fatalf("OpenStorage() error = %v", err)
	}
	defer func() { _ = s.Close() }()

	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Type: raftpb.EntryNormal, Data: []byte("alpha")},
		{Index: 2, Term: 1, Type: raftpb.EntryNormal, Data: []byte("beta")},
	}
	if err := s.Append(entries); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	last, err := s.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex() error = %v", err)
	}
	if last != 2 {
		t.Fatalf("LastIndex() = %d, want 2", last)
	}

	term, err := s.Term(2)
	if err != nil {
		t.Fatalf("Term(2) error = %v", err)
	}
	if term != 1 {
		t.Fatalf("Term(2) = %d, want 1", term)
	}

	got, err := s.Entries(1, 3, 0)
	if err != nil {
		t.Fatalf("Entries(1, 3, 0) error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Entries count = %d, want 2", len(got))
	}
	if string(got[0].Data) != "alpha" || string(got[1].Data) != "beta" {
		t.Fatalf("Entries data = %q, %q; want alpha, beta", got[0].Data, got[1].Data)
	}
}

func TestStorage_Entries_MaxSize(t *testing.T) {
	t.Parallel()

	s, err := OpenStorage(t.TempDir(), 1)
	if err != nil {
		t.Fatalf("OpenStorage() error = %v", err)
	}
	defer func() { _ = s.Close() }()

	if err := s.Append([]raftpb.Entry{
		{Index: 1, Term: 1, Type: raftpb.EntryNormal, Data: []byte("aaaa")},
		{Index: 2, Term: 1, Type: raftpb.EntryNormal, Data: []byte("bbbbbbbb")},
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Small maxSize: first entry fits, second would exceed limit.
	got, err := s.Entries(1, 3, 32)
	if err != nil {
		t.Fatalf("Entries() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Entries count = %d, want 1", len(got))
	}
	if got[0].Index != 1 {
		t.Fatalf("first entry index = %d, want 1", got[0].Index)
	}
}

func TestStorage_SaveHardStateRoundTrip(t *testing.T) {
	t.Parallel()

	s, err := OpenStorage(t.TempDir(), 1)
	if err != nil {
		t.Fatalf("OpenStorage() error = %v", err)
	}
	defer func() { _ = s.Close() }()

	want := raftpb.HardState{Term: 5, Vote: 1, Commit: 42}
	if err := s.SaveHardState(want); err != nil {
		t.Fatalf("SaveHardState() error = %v", err)
	}

	hs, cs, err := s.InitialState()
	if err != nil {
		t.Fatalf("InitialState() error = %v", err)
	}
	if hs != want {
		t.Fatalf("HardState = %+v, want %+v", hs, want)
	}
	if !reflect.DeepEqual(cs.Voters, []uint64{1}) {
		t.Fatalf("ConfState.Voters = %v, want [1]", cs.Voters)
	}
}

func TestOpenStorage_ReopenRebuildsIndex(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	s1, err := OpenStorage(dir, 1)
	if err != nil {
		t.Fatalf("OpenStorage #1 error = %v", err)
	}
	if err := s1.Append([]raftpb.Entry{
		{Index: 1, Term: 2, Type: raftpb.EntryNormal, Data: []byte("persist")},
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	s2, err := OpenStorage(dir, 1)
	if err != nil {
		t.Fatalf("OpenStorage #2 error = %v", err)
	}
	defer func() { _ = s2.Close() }()

	last, err := s2.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex() error = %v", err)
	}
	if last != 1 {
		t.Fatalf("LastIndex() = %d, want 1", last)
	}

	term, err := s2.Term(1)
	if err != nil {
		t.Fatalf("Term(1) error = %v", err)
	}
	if term != 2 {
		t.Fatalf("Term(1) = %d, want 2", term)
	}

	got, err := s2.Entries(1, 2, 0)
	if err != nil {
		t.Fatalf("Entries() error = %v", err)
	}
	if len(got) != 1 || string(got[0].Data) != "persist" {
		t.Fatalf("Entries = %+v, want one entry with data %q", got, "persist")
	}
}

func TestStorage_Term_Errors(t *testing.T) {
	t.Parallel()

	s, err := OpenStorage(t.TempDir(), 1)
	if err != nil {
		t.Fatalf("OpenStorage() error = %v", err)
	}
	defer func() { _ = s.Close() }()

	if _, err := s.Term(0); err != raft.ErrCompacted {
		t.Fatalf("Term(0) error = %v, want ErrCompacted", err)
	}
	if _, err := s.Term(1); err != raft.ErrUnavailable {
		t.Fatalf("Term(1) error = %v, want ErrUnavailable", err)
	}
}

func TestStorage_Entries_EmptyRange(t *testing.T) {
	t.Parallel()

	s, err := OpenStorage(t.TempDir(), 1)
	if err != nil {
		t.Fatalf("OpenStorage() error = %v", err)
	}
	defer func() { _ = s.Close() }()

	got, err := s.Entries(1, 1, 0)
	if err != nil {
		t.Fatalf("Entries(1, 1, 0) error = %v", err)
	}
	if got != nil {
		t.Fatalf("Entries(1, 1, 0) = %v, want nil", got)
	}
}
