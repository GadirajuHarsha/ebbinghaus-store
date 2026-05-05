# ebbinghaus-store

Ebbinghaus Cognition-Optimized Distributed Storage Engine

## Project Vision
This project integrates the **Ebbinghaus Forgetting Curve** into an LSM-Tree storage architecture. Data in this engine isn't just "hot" or "cold" based on recent writes. Instead, it has a **Retention Strength** that grows with every read (rehearsal) and decays over time.

---

## Current Status:
We have completed the foundational storage layer. Currently, our engine:
1.  Logs every operation to a performant binary write-ahead log (WAL).
    -  Headers are 32-byte aligned with CRC32 checksums for corruption detection.
2.  Uses a BTree-based MemTable for log_n lookup and sorted iteration.
3.  Uses "tombstone" records to ensure deletes are persistent across storage tiers.
4.  Implements R = e^(-t/S) to score the "importance" of every record on disk.
5.  Tracks decay curves with millisecond-precision
6.  Maintains the `sst/` directory for immutable sorted files (SSTable's).

---

## Getting Started

### Prerequisites
*   Rust (latest stable)
*   Cargo

### Running the Test Suite
The engine comes with an automated test suite in `src/main.rs` that demonstrates the full lifecycle of a cognitive record.
```bash
cargo run
```

### Understanding the Output
*   **Retention Strength 1.0000**: Data was recently accessed or has high stability.
*   **Retention Strength < 1.0000**: Data is beginning to be "forgotten" by the engine.

---

## Roadmap

### Next: Persistence & Indexing
*   **Persistent Search**: Search through SSTables on disk if a key is missing from RAM.
*   **Manifest Management**: A system to track which `.sst` files belong to which generation.
*   **Bloom Filters**: Speed up disk lookups by 10x using probabilistic filtering.

### Later: Cognitive Compaction
*   **Merge & Forget**: Background threads that merge SSTables and permanently delete "forgotten" data.
*   **Tiered Movement**: Automatically moving high-strength data to NVMe and low-strength data to HDD/S3.

### Later: Distributed Consensus
*   **gRPC Layer**: Inter-node communication.
*   **Consensus**: Implementing a simplified Raft or Paxos for data replication.
