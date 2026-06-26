package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/carissaayo/go-kv-dist/internal/node"
)

func main() {
	id := flag.Uint64("id", 1, "raft node ID")
	dataDir := flag.String("data", "./data/node1", "data directory")
	flag.Parse()

	n, err := node.NewNode(*dataDir, *id)
	if err != nil {
		log.Fatalf("start node: %v", err)
	}
	defer n.Stop()

	log.Printf("kvd: node %d running (phase 1); data=%s", *id, *dataDir)
	log.Printf("kvd: status=%+v", n.Status())

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("kvd: node %d running (phase 1); data=%s", *id, *dataDir)

	if err := waitUntilLeader(ctx, n, *id); err != nil {
		log.Fatalf("wait for leader: %v", err)
	}

	log.Printf("kvd: elected leader; status=%+v", n.Status())

	lastBefore, err := n.Storage().LastIndex()
	if err != nil {
		log.Fatalf("last index: %v", err)
	}

	if err := n.Propose(ctx, []byte("noop")); err != nil {
		log.Fatalf("propose: %v", err)
	}

	if err := waitUntilLogGrows(ctx, n, lastBefore); err != nil {
		log.Fatalf("wait for log: %v", err)
	}

	last, err := n.Storage().LastIndex()
	if err != nil {
		log.Fatalf("last index: %v", err)
	}

	log.Printf("kvd: propose committed; lastIndex=%d status=%+v", last, n.Status())

	log.Printf("kvd: restart with same --data to verify RestartNode path")

	<-ctx.Done()

	log.Println("kvd: shutting down")
}

// Wait until this node is leader.
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

// Wait for persistence
func waitUntilLogGrows(ctx context.Context, n *node.Node, prev uint64) error {
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
