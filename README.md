# mini-dist-kv

A 3-node, Raft-replicated key-value service. Writes are committed through [etcd/raft](https://github.com/etcd-io/raft) before being acknowledged, giving linearizable reads from the leader and no data loss after a leader failure. The storage layer is not reimplemented here — it's the existing [`durable-kv-store`](#dependency-durable-kv-store) engine (WAL + snapshot + in-memory map), wired up as the Raft state machine. All client and peer traffic runs over gRPC.

| Target | Value |
|---|---|
| Correctness | No committed data lost after a leader crash |
| Availability | Writes resume within ~2s of leader death |
| Consistency | Linearizable reads from the leader |
| Language | Go 1.21+ |
| Consensus | etcd/raft |
| Transport | gRPC (client API + inter-node Raft messages) |
| Cluster size | 3 nodes |

## Why gRPC instead of HTTP

The original design allowed either HTTP or gRPC for the client API and either gRPC or raw HTTP/2 for the Raft peer transport. This project standardizes on **gRPC for both**, which simplifies the stack to a single proto-defined contract and gives streaming, deadlines, and connection multiplexing for free.

The one design change this forces: HTTP's `307 Temporary Redirect` for leader redirection has no gRPC equivalent. Instead, a non-leader node returns a `FAILED_PRECONDITION` status and attaches the current leader's address as response metadata (e.g. an `x-raft-leader` trailer, or a `leader_hint` field on the response message). A client-side interceptor catches that status, re-dials the leader, and retries — so callers still get "send to any node" behavior without manual redirect handling.

## Dependency: durable-kv-store

This project does not contain its own storage engine. It imports the engine package from the already-built `durable-kv-store` project (WAL, snapshot, CRC32-checked records, `sync.RWMutex` map) and adapts it to satisfy `raft.Storage`. Concretely:

- `go.mod` requires the engine as a module:
  ```
  require github.com/<your-org>/durable-kv-store v0.x.x
  ```
  (swap in your actual module path — use a `replace` directive pointing at a local path while developing both repos side by side.)
- `internal/node/storage.go` wraps the imported WAL/snapshot APIs to implement `InitialState`, `Entries`, `Term`, `LastIndex`, `FirstIndex`, and `Snapshot`.
- `internal/kv/apply.go` decodes committed Raft entries and calls the imported engine's `Get` / `Set` / `Delete` directly — no engine code lives in this repo.

## Architecture

| Layer | Component | Technology | Purpose |
|---|---|---|---|
| Client | gRPC client | grpc-go | Sends `Get` / `Set` / `Delete` to any node |
| Gateway | gRPC service | Go (generated stubs) | Redirects writes to the leader, serves reads locally |
| Consensus | Raft node | etcd/raft | Log replication, leader election, commit notifications |
| State machine | KV apply loop | Go goroutine | Applies committed Raft entries to the KV engine |
| Storage | KV engine | `durable-kv-store` (imported) | In-memory map + WAL + snapshot |
| Transport | Raft peer transport | gRPC | Inter-node Raft message delivery |

### Propose → commit → apply flow

1. Client calls `Set(key, val)` on any node via gRPC.
2. If the node isn't leader, it returns `FAILED_PRECONDITION` with the leader's address attached; the client retries there.
3. The leader calls `node.Propose(ctx, encodedCommand)`.
4. etcd/raft replicates the entry to a majority of nodes.
5. The entry appears in `Ready().CommittedEntries` on the leader (and eventually followers).
6. The apply goroutine decodes the command and calls the imported engine's `Set`.
7. The leader responds to the client with success.

## Proto definitions

Two services live under `proto/`:

- **`kv.proto`** — the client-facing API (`Get`, `Set`, `Delete`), returned status codes for leader redirection, and read-mode selection (leader / follower / ReadIndex).
- **`raft_transport.proto`** — the inter-node service used to ship Raft messages (`MsgApp`, `MsgVote`, snapshots, etc.) between peers in place of raw HTTP/2.

## Project structure

```
mini-dist-kv/
├── cmd/kvd/main.go              # Node bootstrap, flag parsing, signal handling
├── internal/
│   ├── node/
│   │   ├── node.go              # Raft node lifecycle, Ready() loop
│   │   ├── storage.go           # raft.Storage adapter over durable-kv-store's WAL
│   │   └── transport.go         # gRPC peer message send/receive
│   ├── kv/
│   │   ├── apply.go             # Committed entry decode + call into durable-kv-store engine
│   │   └── snapshot.go          # Snapshot trigger + serialize
│   ├── api/
│   │   └── server.go            # gRPC KVService impl: leader check, propose, leader-hint on redirect
│   └── metrics/
│       └── metrics.go           # Prometheus Raft + KV metrics
├── proto/
│   ├── kv.proto                 # Client-facing gRPC service
│   └── raft_transport.proto     # Inter-node Raft transport service
├── docs/
│   └── architecture.md
├── go.mod                       # requires durable-kv-store as a dependency
└── README.md
```

## Raft storage interface

`durable-kv-store`'s WAL backs `raft.Storage` directly — no second log is kept.

| Method | Backed by |
|---|---|
| `InitialState()` | HardState (term, vote, commit) + ConfState read from WAL |
| `Entries(lo, hi, maxSize)` | Log entries in `[lo, hi)` from WAL |
| `Term(i)` | Term of log entry `i` |
| `LastIndex()` | Index of the last WAL entry |
| `FirstIndex()` | Index of first entry after the last snapshot |
| `Snapshot()` | Latest snapshot (serialized KV map + Raft metadata) |

## Running a 3-node cluster

```bash
# Node 1
./kvd --id 1 --addr :8081 --peers 2=localhost:8082,3=localhost:8083 --data ./data/node1

# Node 2
./kvd --id 2 --addr :8082 --peers 1=localhost:8081,3=localhost:8083 --data ./data/node2

# Node 3
./kvd --id 3 --addr :8083 --peers 1=localhost:8081,2=localhost:8082 --data ./data/node3
```

All three flags (`--addr`, `--peers`) refer to gRPC listen addresses — the same port serves both the client `KVService` and the peer `RaftTransport` service.

## Read consistency options

| Mode | Consistency | Implementation |
|---|---|---|
| Leader reads | Linearizable — always reflects the latest committed write | Route all `Get` calls to the current leader |
| Follower reads | Eventually consistent — may be slightly stale | Any node serves `Get` from its local map |
| ReadIndex reads | Linearizable from any node | Call `node.ReadIndex()` to confirm the local log is caught up before serving |

Start with leader reads; ReadIndex is the production-correct approach for serving reads from followers.

## Failure drills

| Scenario | How to test | Expected behavior |
|---|---|---|
| Leader crash | Kill the leader process mid-write | New leader elected in < 2s; writes resume; no committed data lost |
| Follower crash | Kill one follower; keep writing to the leader | Cluster still accepts writes (majority = 2 of 3); crashed node rejoins and catches up |
| Network partition | Block ports between nodes with `iptables`/`tc` | Minority partition rejects writes; majority continues; partition heals and logs converge |
| Leader restart | Start leader, write 100 keys, stop, restart | All 100 keys present after restart; WAL replayed correctly via `durable-kv-store` |
| New node join | Start cluster with 2 nodes, add a 3rd late | 3rd node receives a snapshot and catches up to current state |

## Observability

| Metric | Type | Description |
|---|---|---|
| `raft_leader_changes_total` | Counter | Number of leader elections — a high rate means instability |
| `raft_commit_index` | Gauge | Current committed log index per node |
| `raft_apply_lag` | Gauge | Difference between commit index and last applied index |
| `raft_proposals_total` | Counter | Total proposals (writes) attempted |
| `raft_proposals_failed_total` | Counter | Proposals that failed (not leader, timeout) |
| `kv_snapshot_duration_seconds` | Histogram | Time taken to write a snapshot |

## Implementation phases

| Phase | Goal | Key deliverables |
|---|---|---|
| 1 — Single node | Raft node boots, proposes no-op | etcd/raft wired, `Ready()` loop running, `raft.Storage` adapter over `durable-kv-store`'s WAL |
| 2 — KV apply | Set/Get over a single Raft node | Propose path, apply goroutine, gRPC `KVService`, leader-hint redirect |
| 3 — 3-node cluster | Replication working | gRPC `RaftTransport` service, cluster bootstrap, writes replicated to all 3 nodes |
| 4 — Failure drills | Correctness under failure | Leader crash test, follower rejoin, all failure scenarios in the test suite |
| 5 — Snapshot | Log compaction | Snapshot trigger, install snapshot on a lagging follower, compaction |
| 6 — Observability | Production-grade visibility | Prometheus metrics, structured logs for every state transition |

## Prerequisites

- Go 1.21+
- `protoc` + `protoc-gen-go` / `protoc-gen-go-grpc` (or [buf](https://buf.build)) to generate stubs from `proto/`
- The `durable-kv-store` engine, either as a published module dependency or a local `replace` directive in `go.mod` while iterating on both projects together

## License

TBD.