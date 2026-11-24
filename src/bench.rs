use std::sync::Arc;
use std::time::Instant;
use crate::server::proto::ebbinghaus_service_client::EbbinghausServiceClient;
use crate::server::proto::{GetRequest, PutRequest};

// top-level benchmark entrypoint wrapped in a hard 30-second timeout harness
pub async fn run_benchmark(args: &[String]) -> Result<(), Box<dyn std::error::Error>>
{
    let timeout_duration = std::time::Duration::from_secs(30);
    match tokio::time::timeout(timeout_duration, run_benchmark_inner(args)).await
    {
        Ok(res) => res,
        Err(_) =>
        {
            eprintln!("benchmark error: execution timed out after 30 seconds");
            Err("benchmark timed out".into())
        },
    }
}

async fn run_benchmark_inner(args: &[String]) -> Result<(), Box<dyn std::error::Error>>
{
    // parse command line parameters
    let explicit_addr = args.iter()
        .find(|a| a.starts_with("--addr="))
        .and_then(|a| a.strip_prefix("--addr="))
        .map(|s| s.to_string());

    let total_requests: usize = args.iter()
        .find(|a| a.starts_with("--requests="))
        .and_then(|a| a.strip_prefix("--requests="))
        .and_then(|s| s.parse().ok())
        .unwrap_or(50_000);

    let concurrency: usize = args.iter()
        .find(|a| a.starts_with("--concurrency="))
        .and_then(|a| a.strip_prefix("--concurrency="))
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);

    let read_ratio: usize = args.iter()
        .find(|a| a.starts_with("--read-ratio="))
        .and_then(|a| a.strip_prefix("--read-ratio="))
        .and_then(|s| s.parse().ok())
        .unwrap_or(80);

    let keys_count: usize = args.iter()
        .find(|a| a.starts_with("--keys="))
        .and_then(|a| a.strip_prefix("--keys="))
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000);

    println!("============================================================");
    println!("             ebbinghaus benchmark configuration             ");
    println!("============================================================");
    println!("total operations:    {}", total_requests);
    println!("concurrency:         {} workers", concurrency);
    println!("workload profile:    {}% reads / {}% writes", read_ratio, 100 - read_ratio);
    println!("keyspace size:       {} keys", keys_count);

    // establish target endpoint: use explicit address if provided, otherwise spin up ephemeral server
    let (endpoint, server_handle) = match explicit_addr
    {
        Some(addr) =>
        {
            println!("target address:      {} (external)", addr);
            println!("============================================================");

            let ep = tonic::transport::Endpoint::from_shared(addr.clone())?
                .connect_timeout(std::time::Duration::from_millis(500))
                .timeout(std::time::Duration::from_millis(500));
            let mut client = EbbinghausServiceClient::connect(ep).await?;
            client.get(GetRequest { key: b"__health_check__".to_vec() }).await?;
            (addr, None)
        },
        None =>
        {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let local_addr = listener.local_addr()?;
            let in_process_addr = format!("http://{}", local_addr);
            println!("target address:      {} (in-process)", in_process_addr);
            println!("============================================================");

            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let engine = Arc::new(tokio::sync::Mutex::new(crate::memtable::MemTable::with_capacity(16 * 1024 * 1024)));
            let service = crate::server::StorageServer::new(engine);

            let handle = tokio::spawn(async move
            {
                let _ = tonic::transport::Server::builder()
                    .add_service(crate::server::proto::ebbinghaus_service_server::EbbinghausServiceServer::new(service))
                    .serve_with_incoming(incoming)
                    .await;
            });

            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            (in_process_addr, Some(handle))
        },
    };

    // 1. warm-up phase: populate keyspace
    println!("warming up storage engine with {} keys...", keys_count);
    let warmup_ep = tonic::transport::Endpoint::from_shared(endpoint.clone())?
        .connect_timeout(std::time::Duration::from_secs(1))
        .timeout(std::time::Duration::from_secs(1));
    let mut warmup_client = EbbinghausServiceClient::connect(warmup_ep).await?;

    for i in 0..keys_count
    {
        let key = format!("bench_key_{:06}", i).into_bytes();
        let val = format!("bench_val_{:06}", i).into_bytes();
        let _ = warmup_client.put(PutRequest
        {
            key,
            value: val,
        }).await?;
    }
    println!("warm-up complete. launching benchmark workers...\n");

    // 2. execution phase
    let requests_per_worker = total_requests / concurrency;
    let actual_total = requests_per_worker * concurrency;
    let endpoint_arc = Arc::new(endpoint);

    let start_time = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);

    for worker_id in 0..concurrency
    {
        let url = endpoint_arc.clone();
        let h = tokio::spawn(async move
        {
            let ep = match tonic::transport::Endpoint::from_shared(url.to_string())
            {
                Ok(e) => e.connect_timeout(std::time::Duration::from_secs(2))
                          .timeout(std::time::Duration::from_secs(2)),
                Err(e) =>
                {
                    eprintln!("worker {} invalid endpoint: {}", worker_id, e);
                    return Vec::new();
                },
            };

            let mut client = match EbbinghausServiceClient::connect(ep).await
            {
                Ok(c) => c,
                Err(e) =>
                {
                    eprintln!("worker {} connection error: {}", worker_id, e);
                    return Vec::new();
                },
            };

            let mut worker_latencies = Vec::with_capacity(requests_per_worker);
            for i in 0..requests_per_worker
            {
                let key_index = (worker_id * 1000 + i) % keys_count;
                let key = format!("bench_key_{:06}", key_index).into_bytes();

                let is_read = ((worker_id + i) % 100) < read_ratio;
                let t0 = Instant::now();

                if is_read
                {
                    let _ = client.get(GetRequest
                    {
                        key,
                    }).await;
                }
                else
                {
                    let _ = client.put(PutRequest
                    {
                        key,
                        value: b"bench_payload_bytes".to_vec(),
                    }).await;
                }

                let elapsed_micros = t0.elapsed().as_micros() as u64;
                worker_latencies.push(elapsed_micros);
            }

            worker_latencies
        });

        handles.push(h);
    }

    // gather all latency samples
    let mut all_latencies = Vec::with_capacity(actual_total);
    for h in handles
    {
        if let Ok(lats) = h.await
        {
            all_latencies.extend(lats);
        }
    }

    let total_duration = start_time.elapsed();
    let total_secs = total_duration.as_secs_f64();
    let throughput = actual_total as f64 / total_secs;

    if let Some(h) = server_handle
    {
        h.abort();
    }

    if all_latencies.is_empty()
    {
        eprintln!("benchmark failed: zero latency samples recorded");
        return Ok(());
    }

    // calculate latency percentiles
    all_latencies.sort_unstable();
    let sample_count = all_latencies.len();
    let min_lat = all_latencies[0];
    let max_lat = all_latencies[sample_count - 1];
    let sum: u64 = all_latencies.iter().sum();
    let avg_lat = sum as f64 / sample_count as f64;

    let p50 = all_latencies[sample_count * 50 / 100];
    let p90 = all_latencies[sample_count * 90 / 100];
    let p95 = all_latencies[sample_count * 95 / 100];
    let p99 = all_latencies[sample_count * 99 / 100];

    println!("============================================================");
    println!("              ebbinghaus benchmark summary                  ");
    println!("============================================================");
    println!("total operations:     {}", actual_total);
    println!("concurrency:          {} workers", concurrency);
    println!("elapsed time:         {:.3} s", total_secs);
    println!("sustained throughput: {:.0} req/sec", throughput);
    println!("");
    println!("latency percentiles (round-trip):");
    println!("  min:                {:.3} ms ({} µs)", min_lat as f64 / 1000.0, min_lat);
    println!("  mean:               {:.3} ms ({} µs)", avg_lat / 1000.0, avg_lat as u64);
    println!("  p50 (median):       {:.3} ms ({} µs)", p50 as f64 / 1000.0, p50);
    println!("  p90:                {:.3} ms ({} µs)", p90 as f64 / 1000.0, p90);
    println!("  p95:                {:.3} ms ({} µs)", p95 as f64 / 1000.0, p95);
    println!("  p99 (tail latency): {:.3} ms ({} µs)", p99 as f64 / 1000.0, p99);
    println!("  max:                {:.3} ms ({} µs)", max_lat as f64 / 1000.0, max_lat);
    println!("============================================================");

    if p99 < 1000
    {
        println!("target achieved: sub-1ms tail latency at {:.0} req/sec!", throughput);
    }
    else
    {
        println!("tail latency: {:.3} ms", p99 as f64 / 1000.0);
    }

    Ok(())
}
