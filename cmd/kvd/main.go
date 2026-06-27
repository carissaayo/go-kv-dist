package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/carissaayo/go-kv-dist/internal/api"
	"github.com/carissaayo/go-kv-dist/internal/node"
	pb "github.com/carissaayo/go-kv-dist/proto/kvpb"
)

func main() {
	id := flag.Uint64("id", 1, "raft node ID")
	dataDir := flag.String("data", "./data/node1", "data directory")
	addr := flag.String("addr", "localhost:50051", "gRPC listen address (KV + raft transport)")
	peers := flag.String("peers", "", "peer addresses: id=host:port,id=host:port (other nodes; self uses --addr)")
	listen := flag.String("listen", "", "deprecated alias for --addr")
	flag.Parse()

	listenAddr := *addr
	if *listen != "" {
		listenAddr = *listen
	}

	peerAddrs, err := node.ParsePeerAddrs(*id, listenAddr, *peers)
	if err != nil {
		log.Fatalf("parse peers: %v", err)
	}

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	n, err := node.NewNode(*dataDir, *id, node.Options{PeerAddrs: peerAddrs})
	if err != nil {
		log.Fatalf("start node: %v", err)
	}
	defer n.Stop()

	srv := grpc.NewServer()
	pb.RegisterKVServer(srv, api.NewKVServer(n))
	node.RegisterRaftTransport(srv, n)
	reflection.Register(srv)

	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()
	defer srv.GracefulStop()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := waitForRaftReady(ctx, n); err != nil {
		log.Fatalf("wait for raft: %v", err)
	}
	log.Printf("kvd: node %d up; leader=%d addr=%s data=%s", *id, n.LeaderID(), listenAddr, *dataDir)

	<-ctx.Done()
	log.Println("kvd: shutting down")
}

func waitForRaftReady(ctx context.Context, n *node.Node) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if n.LeaderID() != 0 {
				return nil
			}
		}
	}
}
