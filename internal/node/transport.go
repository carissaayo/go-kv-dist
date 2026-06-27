package node

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	rtpb "github.com/carissaayo/go-kv-dist/proto/rafttransportpb"
)

// Transport delivers outbound raftpb.Message values to peers over gRPC.
type Transport struct {
	selfID uint64
	addrs  map[uint64]string

	mu      sync.Mutex
	clients map[uint64]rtpb.RaftTransportClient
	conns   map[uint64]*grpc.ClientConn
}

func NewTransport(selfID uint64, addrs map[uint64]string) *Transport {
	return &Transport{
		selfID:  selfID,
		addrs:   addrs,
		clients: make(map[uint64]rtpb.RaftTransportClient),
		conns:   make(map[uint64]*grpc.ClientConn), // cached gRPC connections
	}
}

func (t *Transport) Send(ctx context.Context, msg raftpb.Message) error {
	if msg.To == t.selfID {
		return fmt.Errorf("transport: send to self")
	}

	addr, ok := t.addrs[msg.To]
	if !ok || addr == "" {
		return fmt.Errorf("transport: no address for peer %d", msg.To)
	}

	client, err := t.client(msg.To, addr)
	if err != nil {
		return err
	}

	data, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("transport: marshal message: %w", err)
	}

	_, err = client.Send(ctx, &rtpb.RaftMessage{Data: data})
	return err
}

func (t *Transport) client(id uint64, addr string) (rtpb.RaftTransportClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if c, ok := t.clients[id]; ok {
		return c, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("transport: dial %q: %w", addr, err)
	}

	t.conns[id] = conn
	t.clients[id] = rtpb.NewRaftTransportClient(conn)
	return t.clients[id], nil
}

func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	var first error
	for id, conn := range t.conns {
		if err := conn.Close(); err != nil && first == nil {
			first = err
		}

		delete(t.conns, id)
		delete(t.clients, id)
	}
	return first
}

// Builds id→address map from --peers and includes selfAddr for selfID; format: "2=localhost:50052,3=localhost:50053"
func ParsePeerAddrs(selfID uint64, selfAddr, peersFlag string) (map[uint64]string, error) {
	addrs := map[uint64]string{selfID: selfAddr}
	if peersFlag == "" {
		return addrs, nil
	}

	for _, part := range strings.Split(peersFlag, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("peers: invalid segment %q", part)
		}

		id, err := strconv.ParseUint(strings.TrimSpace(kv[0]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("peers: parse id in %q: %w", part, err)
		}

		addr := strings.TrimSpace(kv[1])
		if addr == "" {
			return nil, fmt.Errorf("peers: empty address in %q", part)
		}

		addrs[id] = addr
	}
	return addrs, nil
}

func raftPeers(addrs map[uint64]string) []raft.Peer {
	ids := make([]uint64, 0, len(addrs))
	for id := range addrs {
		ids = append(ids, id)
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	peers := make([]raft.Peer, len(ids))

	for i, id := range ids {
		peers[i] = raft.Peer{ID: id}
	}

	return peers
}

func isCluster(addrs map[uint64]string) bool {
	return len(addrs) > 1
}

// logSendError is kept as a variable for tests.
var logSendError = func(n *Node, to uint64, err error) {
	log.Printf("node %d: send to %d: %v", n.id, to, err)
}
