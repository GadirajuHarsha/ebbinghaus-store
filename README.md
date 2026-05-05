# ebbinghaus-store

# Project Description:

Ebbinghaus Cognition-Optimized Distributed Storage Engine

Objective: Build a high-performance distributed key-value storage engine in Rust that integrates psychological models of memory decay to optimize data placement and compaction

1. Project Vision & Narrative
This project aims to apply the Ebbinghaus Forgetting Curve to the "hot-cold" data problem in distributed systems. Unlike traditional "write-optimized" LSM-trees, this engine uses a "read-driven" lifecycle where data access frequency determines its durability/storage tier.

2. Core Architecture: LSM-Tree Foundation
The engine will be built on a Log-Structured Merge-tree (LSM-tree) architecture implemented in Rust to make sure I gain a deeper understanding of systems programming.
- Write-Ahead Log: Ensures atomicity + durability; saves writes to disk before updating in-memory state.
- MemTable: High-concurrency in-memory structure (ex: SkipList, B-Tree) for incoming writes and lookups.
- SSTables: Immutable disk-resident files with sorted key-value pairs, indexed for efficient range queries.
- Bloom Filters: Probabilistic data structures per SSTable to minimize unnecessary disk I/O.

3. The Ebbinghaus Innovation: Read-Driven Lifecycle
The defining feature is the Cognitive Compaction Strategy. Data placement is based on "Retention Strength," modeled after the Forgetting Curve formula: R = e^(-t/S)

Storage Tier          Retention Score (R)    Physical Media        Compaction Action
L0 (Working Memory)   High (>0.8)            RAM / NVMe            None; fast access prioritization.
L1-L3 (Short-Term)    Moderate (0.4 - 0.8)   SSD                   Normal compaction; monitoring access.
L4+ (Long-Term)       Low (<0.4)             HDD / S3-compatible   Aggressive; data "forgotten"/archived.

4. Distributed Systems Layer
To scale horizontally, the engine will implement distributed consensus and replication using async I/O.
- Replication: Leader-follower synchronization ensuring high availability and fault tolerance.
- Communication: High-performance gRPC interfaces for inter-node communication and client-server interactions.
- Concurrency: Utilization of Tokio runtime for managing asynchronous tasks and frequent network requests.

5. Technical Specification
Language: Rust - type safety, memory efficiency, and high performance.
Async Runtime: Tokio - managing I/O-bound tasks and thread pooling.
Networking: Tonic / gRPC - low-latency RPC communication between nodes.
Serialization: Serde / Protocol Buffers - efficient data encoding for disk and wire.
Observability: Prometheus / Grafana - tracking tail latency and storage reclamation efficiency.

6. Benchmarking & Performance Goals
The engine will be validated against RocksDB using a custom benchmarking suite to simulate real-world data access patterns (Zipfian distributions).
- Tail Latency: Target sub-1ms p99 latency for 10,000+ requests per second.
- Disk Efficiency: Measuring the reclamation rate of "cold" data compared to size-tiered compaction.
- Recovery Time: Validating WAL replay speed and SSTable index loading.
