# mini-dist-kv

A 3-node, Raft-replicated key-value service. Writes are committed through [etcd/raft](https://github.com/etcd-io/raft) before being acknowledged, giving linearizable reads from the leader and no data loss after a leader failure. The **KV state machine** is not reimplemented here — it uses the existing [`go-durable-kv`](#dependency-go-durable-kv) engine (`Set` / `Get` / `Delete`, WAL + snapshot + in-memory map). **Consensus persistence** is a sibling **`RaftLog`** (`raft.log`) in the same data directory; `internal/node/storage.go` implements `raft.Storage` on top of it. All client and peer traffic runs over gRPC.

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

## Dependency: go-durable-kv

This project does not contain its own KV engine. It imports [`github.com/carissaayo/go-durable-kv`](https://github.com/carissaayo/go-durable-kv) for the **state machine** and a low-level **`RaftLog`** type for **consensus bytes**. The two are deliberately separate:

| Component | Source | Role |
|---|---|---|
| **`Engine`** | `go-durable-kv/internal/engine` | User KV: in-memory map + `wal.log` + `snapshot.gob`; only updated from **committed** apply |
| **`RaftLog`** | `go-durable-kv/internal/raftlog` | Consensus: append-only `raft.log` (CRC-framed opaque payloads); no user keys, no map replay |

Concretely:

- `go.mod` requires the module (use a `replace` directive to a local checkout while developing both repos side by side):
  ```
  require github.com/carissaayo/go-durable-kv v0.x.x
  ```
- `internal/node/storage.go` implements `raft.Storage`: marshals `raftpb.Entry` into `RaftLog`, keeps an in-memory index, persists `HardState` / `ConfState` to `raft_meta`, and coordinates raft snapshots with the engine.
- `internal/kv/apply.go` decodes **committed** raft commands and calls the engine's `Get` / `Set` / `Delete` — never the other way around (proposals do not call `Engine.Set` directly).

## Architecture

| Layer | Component | Technology | Purpose |
|---|---|---|---|
| Client | gRPC client | grpc-go | Sends `Get` / `Set` / `Delete` to any node |
| Gateway | gRPC service | Go (generated stubs) | Redirects writes to the leader, serves reads locally |
| Consensus | Raft node | etcd/raft | Log replication, leader election, commit notifications |
| State machine | KV apply loop | Go goroutine | Applies committed Raft entries to the KV engine |
| Consensus storage | RaftLog | `go-durable-kv` (imported) | `raft.log` — durable raft entry stream |
| KV storage | Engine | `go-durable-kv` (imported) | In-memory map + `wal.log` + `snapshot.gob` |
| Transport | Raft peer transport | gRPC | Inter-node Raft message delivery |

### Propose → commit → apply flow

1. Client calls `Set(key, val)` on any node via gRPC.
2. If the node isn't leader, it returns `FAILED_PRECONDITION` with the leader's address attached; the client retries there.
3. The leader calls `node.Propose(ctx, encodedCommand)`.
4. etcd/raft replicates the entry to a majority of nodes; each node's `Ready()` loop persists new entries to **`raft.log`** via `RaftLog`.
5. The entry appears in `Ready().CommittedEntries` on the leader (and eventually followers).
6. The apply goroutine decodes the command and calls the imported engine's `Set` (which appends to **`wal.log`**).
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
│   │   ├── storage.go           # raft.Storage over RaftLog + raft_meta; snapshot ties to Engine
│   │   └── transport.go         # gRPC peer message send/receive
│   ├── kv/
│   │   ├── apply.go             # Committed entry decode + call into go-durable-kv Engine
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
├── go.mod                       # requires go-durable-kv as a dependency
└── README.md
```

## Persistence layout

Each node stores consensus and KV data under `--data`. Two logs, one responsibility each:

| File | Component | Contents |
|---|---|---|
| `raft.log` | `RaftLog` | CRC-framed serialized `raftpb.Entry` records (consensus log) |
| `raft_meta` | `storage.go` | `HardState`, `ConfState`, compact watermark (`FirstIndex`) |
| `wal.log` | `Engine` | KV `Set` / `Delete` records (written only after commit + apply) |
| `snapshot.gob` | `Engine` | KV map checkpoint |

Phase 1 may create only `raft.log` and `raft_meta`; `wal.log` appears once the engine is opened and KV commands are applied (Phase 2).

## Raft storage interface

`raft.Storage` is implemented in this repo on top of **`RaftLog`**, not the engine's KV WAL.

| Method | Backed by |
|---|---|
| `InitialState()` | `raft_meta` — `HardState` (term, vote, commit) + `ConfState` |
| `Entries(lo, hi, maxSize)` | In-memory index → read payloads from `raft.log` |
| `Term(i)` | Entry at index `i` from the index / `raft.log` |
| `LastIndex()` | Last appended raft log index |
| `FirstIndex()` | First index after the last raft snapshot / compaction |
| `Snapshot()` | Latest raft snapshot: metadata + serialized KV map from `Engine` |

Raft log compaction (`RaftLog` truncate) and KV WAL compaction (`Engine` snapshot) are coordinated at snapshot time but triggered by different rules (raft commit index vs engine WAL size).

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
| Leader restart | Start leader, write 100 keys, stop, restart | All 100 keys present after restart; `raft.log` + `raft_meta` restore consensus; engine replays `wal.log` / `snapshot.gob` |
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
| 1 — Single node | Raft node boots, proposes no-op | etcd/raft wired, `Ready()` loop running, `raft.Storage` over `RaftLog` + `raft_meta` |
| 2 — KV apply | Set/Get over a single Raft node | Propose path, apply goroutine, gRPC `KVService`, leader-hint redirect |
| 3 — 3-node cluster | Replication working | gRPC `RaftTransport` service, cluster bootstrap, writes replicated to all 3 nodes |
| 4 — Failure drills | Correctness under failure | Leader crash test, follower rejoin, all failure scenarios in the test suite |
| 5 — Snapshot | Log compaction | Snapshot trigger, install snapshot on a lagging follower, compaction |
| 6 — Observability | Production-grade visibility | Prometheus metrics, structured logs for every state transition |

## Prerequisites

- Go 1.21+
- `protoc` + `protoc-gen-go` / `protoc-gen-go-grpc` (or [buf](https://buf.build)) to generate stubs from `proto/`
- [`go-durable-kv`](https://github.com/carissaayo/go-durable-kv) (`Engine` + `RaftLog`), either as a published module dependency or a local `replace` directive in `go.mod` while iterating on both repos together

## License

TBD.