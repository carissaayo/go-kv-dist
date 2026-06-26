package kv

import (
	"fmt"

	"github.com/carissaayo/go-durable-kv/pkg/engine"
)

// Apply runs a committed raft command against the KV engine.
func Apply(eng *engine.Engine, data []byte) error {
	cmd, err := Decode(data)
	if err != nil {
		// Phase 1 legacy proposals (e.g. []byte("noop")) — not KV commands.
		return nil
	}

	switch cmd.Op {
	case OpSet:
		return eng.Set(cmd.Key, cmd.Value)
	case OpDelete:
		return eng.Delete(cmd.Key)
	default:
		return fmt.Errorf("kv: unsupported op %d", cmd.Op)
	}
}
