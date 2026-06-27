package api

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/carissaayo/go-kv-dist/internal/node"
	pb "github.com/carissaayo/go-kv-dist/proto/kvpb"
)

type KVServer struct {
	pb.UnimplementedKVServer
	node *node.Node
}

func NewKVServer(n *node.Node) *KVServer {
	return &KVServer{node: n}
}

func (s *KVServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	val, found, err := s.node.Get(req.GetKey())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	return &pb.GetResponse{Found: found, Value: val}, nil
}

func (s *KVServer) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if err := s.node.Set(ctx, req.GetKey(), req.GetValue()); err != nil {
		return nil, status.Errorf(codes.Internal, "set: %v", err)
	}
	return &pb.SetResponse{}, nil
}

func (s *KVServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if err := s.node.Delete(ctx, req.GetKey()); err != nil {
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	return &pb.DeleteResponse{}, nil
}

func (s *KVServer) requireLeader() error {
	lead := s.node.LeaderID()
	if lead != s.node.ID() {
		addr := s.node.LeaderAddr()
		if addr != "" {
			return status.Errorf(codes.FailedPrecondition, "not leader; leader_id=%d leader_addr=%s", lead, addr)
		}
		return status.Errorf(codes.FailedPrecondition, "not leader; leader_id=%d", lead)
	}
	return nil
}
