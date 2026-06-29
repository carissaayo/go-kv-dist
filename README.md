# go-kv-dist

A 3-node Raft-replicated key-value service written in Go. Writes are committed through [etcd/raft](https://github.com/etcd-io/raft) before being acknowledged. The KV state machine is provided by [`go-durable-kv`](https://github.com/carissaayo/go-durable-kv); this repository implements consensus, replication, snapshots, and the gRPC API.

| | |
|---|---|
| **Consensus** | etcd/raft (3-node cluster) |
| **Transport** | gRPC — client API and inter-node Raft messages on the same port |
| **Storage** | Separate consensus log (`raft.log`) and KV engine (`wal.log` + `snapshot.gob`) |
| **Observability** | Prometheus metrics + structured logs (`log/slog`) |

## How it works

1. A client calls `Set` or `Delete` on any node.
2. If the node is not the leader, it returns `FAILED_PRECONDITION` with the current leader ID and address in the error message.
3. The leader proposes the command to Raft; a majority persists the entry to `raft.log`.
4. On commit, each node applies the command to its local KV engine (appends to `wal.log`).
5. `Get` is served from the local engine on whichever node receives the request (eventually consistent on followers).

```
 Client                    Leader                     Followers
   |                         |                            |
   |  Set(k,v) via gRPC      |                            |
   |------------------------>|  Propose + replicate       |
   |                         |--------------------------->|
   |                         |  Commit + apply to engine  |
   |  OK                     |                            |
   |<------------------------|                            |
```

Raft log compaction and KV snapshots are coordinated: when the leader compacts old log entries, lagging followers can catch up via a Raft snapshot that carries a serialized copy of the KV map.

## Quick start

### Prerequisites

- Go 1.21+
- [`go-durable-kv`](https://github.com/carissaayo/go-durable-kv) — listed in `go.mod`; use the `replace` directive for a local checkout while developing both repos
- `protoc` + `protoc-gen-go` / `protoc-gen-go-grpc` (only needed to regenerate stubs from `proto/`)

### Build

```bash
go build -o kvd ./cmd/kvd
```

### Run a 3-node cluster

Use `127.0.0.1` on Windows to avoid IPv6/`localhost` resolution issues. Start all three nodes within a few seconds so election stabilizes quickly.

```bash
# Terminal 1
./kvd --id 1 --addr 127.0.0.1:50051 --peers 2=127.0.0.1:50052,3=127.0.0.1:50053 --data ./data/node1 --metrics 127.0.0.1:9091

# Terminal 2
./kvd --id 2 --addr 127.0.0.1:50052 --peers 1=127.0.0.1:50051,3=127.0.0.1:50053 --data ./data/node2 --metrics 127.0.0.1:9092

# Terminal 3
./kvd --id 3 --addr 127.0.0.1:50053 --peers 1=127.0.0.1:50051,2=127.0.0.1:50052 --data ./data/node3 --metrics 127.0.0.1:9093
```

Each process listens on one gRPC address for both the client `KV` service and the peer `RaftTransport` service. Prometheus metrics are exposed on a separate HTTP port (see below).

### `kvd` flags

| Flag | Default | Description |
|---|---|---|
| `--id` | `1` | Raft node ID |
| `--addr` | `localhost:50051` | gRPC listen address (KV + Raft transport) |
| `--peers` | | Peer map: `id=host:port,id=host:port` (omit self; self uses `--addr`) |
| `--data` | `./data/node1` | Data directory for this node |
| `--metrics` | `localhost:9090` | Prometheus `/metrics` listen address (`""` disables) |
| `--listen` | | Deprecated alias for `--addr` |

### Example client calls

With [grpcurl](https://github.com/fullstorydev/grpcurl) (reflection is enabled):

```bash
# Write (must hit the leader, or read the redirect error for leader_addr)
grpcurl -plaintext -d '{"key":"foo","value":"aGVsbG8="}' 127.0.0.1:50051 kvpb.KV/Set

# Read (any node)
grpcurl -plaintext -d '{"key":"foo"}' 127.0.0.1:50051 kvpb.KV/Get

# Delete
grpcurl -plaintext -d '{"key":"foo"}' 127.0.0.1:50051 kvpb.KV/Delete
```

`Set` values are raw bytes (base64 in JSON). A non-leader responds with gRPC status `FailedPrecondition` and a message like `not leader; leader_id=2 leader_addr=127.0.0.1:50052`.

## API

Defined in `proto/kv.proto`:

| RPC | Behavior |
|---|---|
| `Get` | Local read from the node's KV engine |
| `Set` | Leader only; proposed to Raft and waited until applied |
| `Delete` | Leader only; proposed to Raft and waited until applied |

Inter-node Raft messages are defined in `proto/raft_transport.proto` (`RaftTransport.Send`).

## Persistence

Each node's `--data` directory holds two independent storage layers:

| File | Component | Contents |
|---|---|---|
| `raft.log` | `RaftLog` | CRC-framed serialized `raftpb.Entry` records |
| `raft_meta` | `RaftMeta` | `HardState` + `ConfState` |
| `raft_snap_meta` | `Storage` | Snapshot/compaction watermark (`snapIndex`, `snapTerm`, `firstIndex`) |
| `wal.log` | `Engine` | KV `Set` / `Delete` records (written only after commit) |
| `snapshot.gob` | `Engine` | KV map checkpoint |

`internal/node/storage.go` implements `raft.Storage` on top of `RaftLog`. Snapshot creation serializes the engine state into `raftpb.Snapshot.Data`; installation restores the engine and truncates the local raft log.

## Observability

Structured logs (`log/slog`) cover leader changes, log compaction, snapshot install, conf changes, and proposal failures.

Prometheus metrics are labeled with `node_id`:

| Metric | Type | Description |
|---|---|---|
| `raft_leader_changes_total` | Counter | Leader failovers observed by this node |
| `raft_commit_index` | Gauge | Current committed log index |
| `raft_apply_lag` | Gauge | `commit_index − last_applied` |
| `raft_proposals_total` | Counter | Write proposals attempted |
| `raft_proposals_failed_total` | Counter | Proposals rejected or timed out |
| `kv_snapshot_duration_seconds` | Histogram | Time to create or install a KV snapshot |

```bash
curl http://127.0.0.1:9091/metrics
```

## Resilience

The test suite (`go test ./internal/node/...`) covers multi-node replication, leader crash and restart, follower stop/rejoin, quorum writes with one peer down, and snapshot catch-up for a lagging follower. Network partitions and late cluster expansion are not automated in CI; validate those manually if needed.

## Project layout

```
go-kv-dist/
├── cmd/kvd/main.go                 # Node process: flags, gRPC, metrics HTTP, signals
├── internal/
│   ├── api/server.go               # gRPC KV service (leader check on writes)
│   ├── kv/
│   │   ├── apply.go                # Decode committed commands → engine
│   │   ├── command.go              # Set/Delete command encoding
│   │   └── snapshot.go             # KV state encode/decode for Raft snapshots
│   ├── metrics/metrics.go          # Prometheus metric definitions
│   └── node/
│       ├── node.go                 # Lifecycle, bootstrap, accessors
│       ├── node_ready.go           # Tick loop, Ready loop, message send
│       ├── node_kv.go              # Propose, apply, Set/Delete/Get
│       ├── node_snapshot.go        # Install snapshot, compaction trigger
│       ├── node_observe.go         # Metrics + structured logging hooks
│       ├── storage.go              # raft.Storage over RaftLog
│       ├── storage_snap.go         # Snapshots, compaction, snap metadata
│       ├── raft_meta.go            # HardState / ConfState persistence
│       ├── transport.go            # Outbound gRPC to peers
│       └── raft_transport_server.go
├── proto/
│   ├── kv.proto
│   └── raft_transport.proto
└── go.mod
```

## Development

```bash
# Run all tests
go test ./...

# Regenerate protobuf stubs (from repo root)
protoc --go_out=. --go-grpc_out=. proto/kv.proto proto/raft_transport.proto
```

### Dependency: go-durable-kv

| Component | Role in this repo |
|---|---|
| `Engine` | User KV: in-memory map + `wal.log` + `snapshot.gob`; updated only from committed apply |
| `RaftLog` | Consensus: append-only `raft.log`; opaque `raftpb.Entry` payloads |

Proposals never call `Engine.Set` directly — all mutations flow through Raft commit and `internal/kv/apply.go`.

## License

TBD.
