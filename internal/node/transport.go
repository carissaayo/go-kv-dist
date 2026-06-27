package node

import (
	"context"
	"fmt"

	"sync"

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
