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

	"github.com/carissaayo/go-kv-dist/internal/api"
	"github.com/carissaayo/go-kv-dist/internal/node"
	pb "github.com/carissaayo/go-kv-dist/proto"
)

func main() {
	id := flag.Uint64("id", 1, "raft node ID")
	dataDir := flag.String("data", "./data/node1", "data directory")
	listen := flag.String("listen", "localhost:50051", "gRPC listen address")
	flag.Parse()

	n, err := node.NewNode(*dataDir, *id)
	if err != nil {
		log.Fatalf("start node: %v", err)
	}
	defer n.Stop()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := waitUntilLeader(ctx, n, *id); err != nil {
		log.Fatalf("wait for leader: %v", err)
	}
	log.Printf("kvd: node %d leader; data=%s listen=%s", *id, *dataDir, *listen)

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	pb.RegisterKVServer(srv, api.NewKVServer(n))

	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()
	defer srv.GracefulStop()

	<-ctx.Done()
	log.Println("kvd: shutting down")
}

func waitUntilLeader(ctx context.Context, n *node.Node, id uint64) error {
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
