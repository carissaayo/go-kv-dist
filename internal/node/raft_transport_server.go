package node

import (
	"context"
	"fmt"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"

	rtpb "github.com/carissaayo/go-kv-dist/proto/rafttransportpb"
)

type raftTransportServer struct {
	rtpb.UnimplementedRaftTransportServer
	node *Node
}

func (s *raftTransportServer) Send(ctx context.Context, req *rtpb.RaftMessage) (*rtpb.RaftMessageAck, error) {
	var msg raftpb.Message
	if err := msg.Unmarshal(req.GetData()); err != nil {
		return nil, fmt.Errorf("raft transport: unmarshal: %w", err)
	}

	if err := s.node.Step(ctx, msg); err != nil {
		return nil, err
	}

	return &rtpb.RaftMessageAck{}, nil
}

// RegisterRaftTransport mounts the peer message handler on a gRPC server.
func RegisterRaftTransport(s grpc.ServiceRegistrar, n *Node) {
	rtpb.RegisterRaftTransportServer(s, &raftTransportServer{node: n})
}
