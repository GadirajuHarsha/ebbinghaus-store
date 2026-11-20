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
8.  Implements **Leveled Cognitive Compaction** with a multi-way merge, de-duplicating keys and dropping forgotten memories below the oblivion threshold.
9.  Integrates **Bloom Filters** (Kirsch-Mitzenmacher double-hashing) with in-memory caching and companion `.bloom` disk persistence to skip unnecessary disk scans.

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
