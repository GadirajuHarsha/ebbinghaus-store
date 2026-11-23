# ebbinghaus-store

Ebbinghaus Cognition-Optimized Distributed Storage Engine

## Project Vision
This project integrates the **Ebbinghaus Forgetting Curve** into an LSM-Tree storage architecture. Data in this engine isn't just "hot" or "cold" based on recent writes. Instead, it has a **Retention Strength** that grows with every read (rehearsal) and decays over time.

---

## Current Status:
We have completed the core storage engine. Currently, our engine:
1.  Logs every operation to a performant binary write-ahead log (WAL).
    -  Headers are 32-byte aligned with CRC32 checksums for corruption detection.
2.  Uses a BTree-based MemTable for log_n lookup and sorted iteration.
3.  Uses "tombstone" records to ensure deletes are persistent across storage tiers.
4.  Implements R = e^(-t/S) to score the "importance" of every record on disk.
5.  Tracks decay curves with millisecond-precision.
6.  Maintains tiered directories (`sst/tier0`, `sst/tier1`, `sst/tier2`) for persistent SSTables.
7.  Features a persistent reverse-chronological search path with automatic memory promotion (rehearsal).
8.  Implements **Leveled Compaction** with a multi-way merge, de-duplicating keys and dropping forgotten memories below the oblivion threshold.
9.  Integrates **Bloom Filters** (Kirsch-Mitzenmacher double-hashing) with in-memory caching and companion `.bloom` disk persistence to skip unnecessary disk scans.
10. Exposes an asynchronous **gRPC service layer** (Tokio + Tonic) with a Protobuf contract (`proto/ebbinghaus.proto`) supporting concurrent network requests (`Get`, `Put`, `Delete`) and replication primitives.

---

## Getting Started

### Prerequisites
*   Rust (latest stable)
*   Cargo
*   Protocol Buffers (`protoc`)

### Running the Test Suite
The engine comes with an automated 8-stage test suite in `src/main.rs` that demonstrates the full lifecycle of a record, including persistence, compaction, bloom filtering, and live gRPC network operations.
```bash
cargo run
```

### Running as a gRPC Server Daemon
You can launch the engine as a standalone network service:
```bash
cargo run -- --server
```

### Understanding the Output
*   **Retention Strength 1.0000**: Data was recently accessed or has high stability.
*   **Retention Strength < 1.0000**: Data is beginning to be "forgotten" by the engine.

---

## Roadmap

### Current Focus: Distributed Replication & Observability
*   **Leader-Follower Replication**: Streaming WAL updates asynchronously from leader to followers over gRPC.
*   **Prometheus Telemetry**: Real-time histograms for read/write latencies and gauges for retention decay and memory evictions.
*   **High-Throughput Benchmarking**: Validating sub-1ms p99 tail latency under 10k+ req/sec loads.
