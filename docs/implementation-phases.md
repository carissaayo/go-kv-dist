## Implementation phases

| Phase | Goal | Key deliverables |
|---|---|---|
| 1 — Single node | Raft node boots, proposes no-op | etcd/raft wired, `Ready()` loop running, `raft.Storage` adapter over `durable-kv-store`'s WAL |
| 2 — KV apply | Set/Get over a single Raft node | Propose path, apply goroutine, gRPC `KVService`, leader-hint redirect |
| 3 — 3-node cluster | Replication working | gRPC `RaftTransport` service, cluster bootstrap, writes replicated to all 3 nodes |
| 4 — Failure drills | Correctness under failure | Leader crash test, follower rejoin, all failure scenarios in the test suite |
| 5 — Snapshot | Log compaction | Snapshot trigger, install snapshot on a lagging follower, compaction |
| 6 — Observability | Production-grade visibility | Prometheus metrics, structured logs for every state transition |
