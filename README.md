# Ebbinghaus: Distributed KV Storage Engine

High-performance distributed storage engine in Rust featuring an LSM-tree architecture, exponential time-decay compaction, asynchronous gRPC services, and leader-follower replication.

Built in Rust, Tokio, Tonic (gRPC), Prost, and Serde.

---

## Overview

In traditional storage engines, data eviction and caching rely on generic algorithms like LRU (Least Recently Used) or LFU (Least Frequently Used). I designed Ebbinghaus Store to solve the problem of storing and retrieving high-volume contextual information (such as AI context buffers and conversational memory) where items lose relevance over time unless explicitly rehearsed or retrieved.

The storage engine implements an LSM-tree (Log-Structured Merge-tree) where records are scored using Hermann Ebbinghaus's exponential forgetting curve:

R = e^(-t / S)

Every read query acts as a rehearsal event, resetting the decay timer and boosting the record's stability factor S. During background leveled compaction passes, records whose retention score falls below a set oblivion threshold are permanently evicted, keeping active working memory lean while preserving high-frequency data.

---

## Architectural Deep Dive

If you are looking for an in-depth breakdown of the internal subsystems, binary layout, leveled compaction merge loops, and network protocol design, please refer to the dedicated architecture guide:

**Read the full technical specification in [ARCHITECTURE.md](file:///Users/harsha/Documents/Projects/ebbinghaus-store/ARCHITECTURE.md)**

The architecture document covers:
* The motivation and mathematical foundation of access-weighted exponential decay.
* A primer on core concepts for newcomers: LSM-trees, WAL, Bloom filters, Tokio, and gRPC.
* Complete binary specification of the 32-byte aligned WAL and SSTable headers with CRC32 checksums.
* Multi-tier leveled compaction (Tier 0 to Tier 2) with de-duplication and lowest-tier eviction.
* Kirsch-Mitzenmacher double-hashed Bloom filters for zero-I/O absent key rejections.
* Asynchronous gRPC service layer and thread-safe concurrency model.
* Leader-follower replication and non-blocking background synchronization.
* Prometheus metrics collection for tail latency histograms and decay monitoring.

---

## Core Technical Features

* **High-Throughput Write Path**: Append-only binary write-ahead logging with 32-byte aligned headers and CRC32 payload verification.
* **Lexicographical In-Memory Indexing**: BTreeMap-backed MemTable synchronized with an asynchronous Mutex to handle concurrent reads and mutations.
* **Persistent Reverse-Chronological Search**: Multi-tier SSTable search path that automatically promotes retrieved disk records back into working memory.
* **Zero-I/O Bloom Filtering**: Space-efficient Bloom filters cached in RAM and persisted to disk alongside SSTables, avoiding disk seeks for absent keys.
* **Leveled Compaction & Time-Decay Eviction**: Background K-way merge across sorted files that de-duplicates shadowed keys and drops records below the oblivion threshold.
* **Asynchronous gRPC Interface**: Strongly-typed network service defined in Protocol Buffers and served over HTTP/2 using Tonic and Tokio.
* **Distributed Leader-Follower Replication**: Non-blocking async mutation streaming from leader to follower nodes, with follower write-protection and localized read scaling.
* **Prometheus Observability**: Latency histograms (p50, p95, p99) and decay/eviction counters exposed via standard metrics endpoints.

---

## Quickstart

### Prerequisites
* Rust (latest stable toolchain)
* Cargo
* Protocol Buffers compiler (`protoc`)

```bash
# install protoc on macos via homebrew
brew install protobuf
```

### Running the End-to-End Test Suite
I have built an automated 9-stage verification suite in `src/main.rs` that validates the entire engine lifecycle: basic CRUD, SSTable flushing, crash recovery replay, retention score decay, deep disk retrieval, Bloom filter filtering, multi-way compaction, gRPC client-server operations, and leader-follower replication:

```bash
cargo run
```

To run unit tests (Bloom filter bitsets and serialization):
```bash
cargo test
```

### Running Distributed Nodes
You can launch nodes in specific cluster roles:

```bash
# 1. start a follower node on port 50052
cargo run -- --role=follower --port=50052

# 2. start a leader node on port 50051 that replicates to the follower
cargo run -- --role=leader --port=50051 --followers=http://127.0.0.1:50052
```

---
