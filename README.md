# Ebbinghaus: Distributed KV Storage Engine

Thanks for stopping by to check out my project!

I built Ebbinghaus Store in Rust to explore distributed systems and address an architectural challenge I found interesting: what happens when you take a classical Log-Structured Merge-tree (LSM-tree) and replace traditional LRU/LFU eviction with Hermann Ebbinghaus's exponential forgetting curve, a psychological model of memory decay?

$$R = e^{-t / S}$$

In this engine, every read query acts as a rehearsal event that resets the decay timer and boosts stability $S$. Frequently queried keys stay firmly anchored in fast working memory, while unused keys quietly decay and are permanently evicted during background leveled compaction passes.

Built from scratch in Rust using Tokio, Tonic (gRPC), Prost, and Prometheus.

---

## Architecture & Deep Dive

I wrote a comprehensive, standalone architecture guide complete with interactive Mermaid diagrams covering the mathematical foundation, 32-byte aligned binary WAL specifications, K-way compaction merge loops, Bloom filter hashing, distributed consensus, and Prometheus observability!

**For all deep-dive technical and architectural details, please see [ARCHITECTURE.md](https://github.com/GadirajuHarsha/ebbinghaus-store/blob/main/ARCHITECTURE.md)**.

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

### Running the Test Suite
I built an automated 10-stage test suite in `src/main.rs` that validates the entire engine lifecycle: basic CRUD, SSTable flushes, WAL crash recovery replay, retention score decay, deep disk retrieval, Bloom filters, leveled compaction, gRPC networking, leader-follower replication, and Prometheus metrics export with rehearsal synchronization:

```bash
cargo run
```

To run the unit tests (Bloom filter bitsets and serialization):
```bash
cargo test
```

### Running Distributed Nodes
You can also launch cluster nodes locally in specific roles:

```bash
# 1. start a follower node on port 50052 (with metrics on 9091)
cargo run -- --role=follower --port=50052 --metrics-port=9091 --leader=http://127.0.0.1:50051

# 2. start a leader node on port 50051 that replicates to the follower (with metrics on 9090)
cargo run -- --role=leader --port=50051 --metrics-port=9090 --followers=http://127.0.0.1:50052
```

To scrape Prometheus metrics:
```bash
curl http://127.0.0.1:9090/metrics
```

---

## Thanks for Visiting!

Building this engine was an amazing learning experience in systems programming, concurrency, and distributed systems. If you have any questions or feedback while exploring the code, feel free to reach out or dive into [ARCHITECTURE.md](https://github.com/GadirajuHarsha/ebbinghaus-store/blob/main/ARCHITECTURE.md)!
