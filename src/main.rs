mod wal;
mod memtable;
mod bloom;
mod server;
mod metrics;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>
{
    // check if running as a persistent grpc server daemon
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|arg| arg.starts_with("--role=")) || args.contains(&"--server".to_string())
    {
        let is_follower = args.iter().any(|arg| arg == "--role=follower");
        let port = args.iter()
            .find(|arg| arg.starts_with("--port="))
            .and_then(|arg| arg.strip_prefix("--port="))
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(if is_follower { 50052 } else { 50051 });

        let addr = format!("127.0.0.1:{}", port).parse()?;
        let engine = std::sync::Arc::new(tokio::sync::Mutex::new(memtable::MemTable::new()));

        let service = if is_follower
        {
            println!("ebbinghaus follower node running on {}", addr);
            let follower_service = server::StorageServer::new_follower(engine);

            // if leader url is provided, start background task to periodically flush rehearsals
            if let Some(leader_arg) = args.iter().find(|arg| arg.starts_with("--leader="))
            {
                if let Some(leader_url) = leader_arg.strip_prefix("--leader=")
                {
                    let leader_addr = leader_url.to_string();
                    let follower_srv = follower_service.clone();
                    tokio::spawn(async move
                    {
                        let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
                        loop
                        {
                            interval.tick().await;
                            let _ = follower_srv.sync_rehearsals_to_leader(&leader_addr).await;
                        }
                    });
                }
            }

            follower_service
        }
        else
        {
            println!("ebbinghaus leader node running on {}", addr);
            let s = server::StorageServer::new_leader(engine);
            // register followers passed via --followers=url1,url2
            if let Some(followers_arg) = args.iter().find(|arg| arg.starts_with("--followers="))
            {
                if let Some(urls) = followers_arg.strip_prefix("--followers=")
                {
                    for url in urls.split(',')
                    {
                        if !url.trim().is_empty()
                        {
                            println!("registering follower: {}", url.trim());
                            s.add_follower(url.trim().to_string()).await?;
                        }
                    }
                }
            }
            s
        };

        // start prometheus metrics http server
        let metrics_port = args.iter()
            .find(|arg| arg.starts_with("--metrics-port="))
            .and_then(|arg| arg.strip_prefix("--metrics-port="))
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(if is_follower { 9091 } else { 9090 });

        let metrics_addr: std::net::SocketAddr = format!("127.0.0.1:{}", metrics_port).parse()?;
        let metrics_listener = tokio::net::TcpListener::bind(metrics_addr).await?;
        println!("ebbinghaus prometheus metrics server running on {}", metrics_addr);
        let metrics_registry = service.get_metrics();
        tokio::spawn(async move
        {
            let _ = metrics::start_metrics_server(metrics_listener, metrics_registry).await;
        });

        tonic::transport::Server::builder()
            .add_service(server::proto::ebbinghaus_service_server::EbbinghausServiceServer::new(service))
            .serve(addr)
            .await?;
        return Ok(());
    }

    // clean up old files so tests start fresh
    let _ = std::fs::remove_file("wal.log");
    let _ = std::fs::remove_dir_all("sst");

    println!("--- 1. basic lifecycle test ---");
    test_basic_lifecycle()?;

    println!("\n--- 2. sstable flush test ---");
    test_sstable_flush()?;

    println!("\n--- 3. crash recovery test ---");
    test_recovery()?;

    println!("\n--- 4. ebbinghaus retention test ---");
    test_retention_scores()?;

    println!("\n--- 5. deep memory search test ---");
    test_deep_search()?;

    println!("\n--- 6. bloom filter test ---");
    test_bloom_filter()?;

    println!("\n--- 7. compaction test ---");
    test_compaction()?;

    println!("\n--- 8. grpc network service test ---");
    test_grpc_service().await?;

    println!("\n--- 9. leader-follower replication test ---");
    test_leader_follower_replication().await?;

    println!("\n--- 10. metrics and rehearsal synchronization test ---");
    test_metrics_and_rehearsal_synchronization().await?;

    Ok(())
}

fn test_basic_lifecycle() -> std::io::Result<()>
{
    let mut table = memtable::MemTable::new();
    let key = b"lifecycle_key";
    let value = b"lifecycle_value";

    table.put(wal::RecordType::Put, key, value)?;
    if let Some(res) = table.get(key)
    {
        println!("initial put found: {:?}", String::from_utf8_lossy(res));
    }

    table.delete(key)?;
    if table.get(key).is_none()
    {
        println!("key successfully deleted (tombstone works)");
    }

    table.put(wal::RecordType::Put, key, value)?;
    if let Some(res) = table.get(key)
    {
        println!("second put found: {:?}", String::from_utf8_lossy(res));
    }
    Ok(())
}

fn test_sstable_flush() -> std::io::Result<()>
{
    let mut table = memtable::MemTable::new();
    let key1 = b"large_key_one_for_testing"; 
    let val1 = b"large_value_one_for_testing"; 
    let key2 = b"trigger_key";
    let val2 = b"trigger_val";

    println!("writing records to trigger flush");
    table.put(wal::RecordType::Put, key1, val1)?;
    table.put(wal::RecordType::Put, key2, val2)?;

    println!("check 'sst/' folder for files");
    Ok(())
}

fn test_recovery() -> std::io::Result<()>
{
    println!("simulating crash");
    println!("replaying WAL");
    let records = wal::recover_from_wal()?;
    println!("recovered {} records from the log", records.len());
    
    for (k, _v, t) in records
    {
        let type_str = if t == 0 { "PUT" } else { "DEL" };
        println!("    - found key: {:?} (type: {})", String::from_utf8_lossy(&k), type_str);
    }
    Ok(())
}

fn test_retention_scores() -> std::io::Result<()>
{
    let mut table = memtable::MemTable::new();
    let key_stable = b"stable_memory";
    let key_fading = b"fading_memory";
    
    table.put(wal::RecordType::Put, key_stable, b"important")?;
    table.put(wal::RecordType::Put, key_fading, b"trivial")?;

    println!("rehearsing 'stable_memory' 10 times");
    for _ in 0..10
    {
        let _ = table.get(key_stable);
    }

    println!("flushing to sstable");
    table.create_sstable()?;

    let scores = table.calculate_scores()?;
    for (key, score) in scores
    {
        println!("    - key: {:?}, retention strength: {:.4}", String::from_utf8_lossy(&key), score);
    }
    Ok(())
}

fn test_deep_search() -> std::io::Result<()>
{
    let mut table = memtable::MemTable::new();
    let key_to_persist = b"long_term_key";
    let value_to_persist = b"this_should_be_on_disk";
    
    // fill the memtable so it flushes and clears itself
    println!("putting key into memtable");
    table.put(wal::RecordType::Put, key_to_persist, value_to_persist)?;
    
    println!("triggering flush with a second large record");
    let large_key = b"large_trigger_key_to_force_flush_and_clear_ram";
    table.put(wal::RecordType::Put, large_key, b"ignore_me")?;

    // memtable is now cleared. search disk for the first key
    println!("memtable should be empty. searching disk for 'long_term_key'");
    if let Some(val) = table.get(key_to_persist)
    {
        println!("found key on disk: {:?}", String::from_utf8_lossy(val));
    }
    else
    {
        println!("key not found on disk");
    }
    
    Ok(())
}

fn test_bloom_filter() -> std::io::Result<()>
{
    // test standalone bloom filter
    let mut filter = bloom::BloomFilter::new(512, 3);
    filter.insert(b"present_key");

    println!("standalone bloom membership: {}", filter.contains(b"present_key"));
    println!("standalone bloom non-membership: {}", !filter.contains(b"absent_key"));

    // test bloom filter on disk alongside sstables
    let mut table = memtable::MemTable::new();
    table.put(wal::RecordType::Put, b"bloom_test_key", b"bloom_test_value")?;
    table.create_sstable()?;

    // searching for present key should find it
    if let Some(val) = table.get(b"bloom_test_key")
    {
        println!("found bloom-filtered key: {:?}", String::from_utf8_lossy(val));
    }

    // searching for absent key will be rejected by bloom filter without sstable scan
    if table.get(b"non_existent_bloom_key").is_none()
    {
        println!("non-existent key filtered out by bloom filter");
    }

    Ok(())
}

fn test_compaction() -> std::io::Result<()>
{
    let mut table = memtable::MemTable::new();

    // write multiple records to populate tier0
    println!("populating tier0 sstables");
    table.put(wal::RecordType::Put, b"merge_key_a", b"value_a_old")?;
    table.create_sstable()?;

    table.put(wal::RecordType::Put, b"merge_key_b", b"value_b")?;
    table.create_sstable()?;

    table.put(wal::RecordType::Put, b"merge_key_a", b"value_a_new")?;
    table.create_sstable()?;

    println!("compacting tier0 into tier1");
    table.compact_tier(0)?;

    // verify that tier1 contains the newest version of merged keys
    if let Some(val) = table.get(b"merge_key_a")
    {
        println!("retrieved compacted key_a: {:?}", String::from_utf8_lossy(val));
    }
    if let Some(val) = table.get(b"merge_key_b")
    {
        println!("retrieved compacted key_b: {:?}", String::from_utf8_lossy(val));
    }

    Ok(())
}

async fn test_grpc_service() -> Result<(), Box<dyn std::error::Error>>
{
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use server::proto::ebbinghaus_service_client::EbbinghausServiceClient;
    use server::proto::ebbinghaus_service_server::EbbinghausServiceServer;
    use server::proto::{GetRequest, PutRequest, DeleteRequest};

    let engine = Arc::new(Mutex::new(memtable::MemTable::new()));
    let service = server::StorageServer::new(engine);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    // spawn grpc server in background with pre-bound listener
    let server_handle = tokio::spawn(async move
    {
        let _ = tonic::transport::Server::builder()
            .add_service(EbbinghausServiceServer::new(service))
            .serve_with_incoming(incoming)
            .await;
    });

    // connect client over async channel
    let client_addr = format!("http://{}", addr);
    let mut client = EbbinghausServiceClient::connect(client_addr).await?;

    // test put over grpc
    println!("putting key over grpc");
    let put_res = client.put(PutRequest
    {
        key: b"network_key".to_vec(),
        value: b"network_value".to_vec(),
    }).await?;
    println!("put acknowledged: {}", put_res.into_inner().success);

    // test get over grpc
    println!("getting key over grpc");
    let get_res = client.get(GetRequest
    {
        key: b"network_key".to_vec(),
    }).await?;
    let body = get_res.into_inner();
    println!("get found: {}, value: {:?}", body.found, String::from_utf8_lossy(&body.value));

    // test delete over grpc
    println!("deleting key over grpc");
    let del_res = client.delete(DeleteRequest
    {
        key: b"network_key".to_vec(),
    }).await?;
    println!("delete acknowledged: {}", del_res.into_inner().success);

    // verify key is now absent
    let get_after_del = client.get(GetRequest
    {
        key: b"network_key".to_vec(),
    }).await?;
    println!("get after delete found: {}", get_after_del.into_inner().found);

    server_handle.abort();
    Ok(())
}

async fn test_leader_follower_replication() -> Result<(), Box<dyn std::error::Error>>
{
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use server::proto::ebbinghaus_service_client::EbbinghausServiceClient;
    use server::proto::ebbinghaus_service_server::EbbinghausServiceServer;
    use server::proto::{GetRequest, PutRequest};

    println!("initializing leader and follower nodes");

    // 1. start follower on ephemeral port
    let follower_engine = Arc::new(Mutex::new(memtable::MemTable::new()));
    let follower_service = server::StorageServer::new_follower(follower_engine);
    let follower_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let follower_addr = follower_listener.local_addr()?;
    let follower_incoming = tokio_stream::wrappers::TcpListenerStream::new(follower_listener);

    let follower_handle = tokio::spawn(async move
    {
        let _ = tonic::transport::Server::builder()
            .add_service(EbbinghausServiceServer::new(follower_service))
            .serve_with_incoming(follower_incoming)
            .await;
    });

    // 2. start leader on ephemeral port and register follower
    let leader_engine = Arc::new(Mutex::new(memtable::MemTable::new()));
    let leader_service = server::StorageServer::new_leader(leader_engine);
    let follower_url = format!("http://{}", follower_addr);
    leader_service.add_follower(follower_url).await?;

    let leader_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let leader_addr = leader_listener.local_addr()?;
    let leader_incoming = tokio_stream::wrappers::TcpListenerStream::new(leader_listener);

    let leader_handle = tokio::spawn(async move
    {
        let _ = tonic::transport::Server::builder()
            .add_service(EbbinghausServiceServer::new(leader_service))
            .serve_with_incoming(leader_incoming)
            .await;
    });

    // 3. connect client to leader and write data
    let mut leader_client = EbbinghausServiceClient::connect(format!("http://{}", leader_addr)).await?;
    println!("writing key 'replicated_key' to leader");
    let put_res = leader_client.put(PutRequest
    {
        key: b"replicated_key".to_vec(),
        value: b"replicated_value".to_vec(),
    }).await?;
    println!("leader put acknowledged: {}", put_res.into_inner().success);

    // 4. wait a small moment for async network replication to propagate
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // 5. connect client directly to follower and read replicated data
    println!("querying follower directly for 'replicated_key'");
    let mut follower_client = EbbinghausServiceClient::connect(format!("http://{}", follower_addr)).await?;
    let get_res = follower_client.get(GetRequest
    {
        key: b"replicated_key".to_vec(),
    }).await?;

    let body = get_res.into_inner();
    println!("follower read successful: {}, value: {:?}", body.found, String::from_utf8_lossy(&body.value));
    assert!(body.found);
    assert_eq!(body.value, b"replicated_value");

    // 6. verify write rejection on follower
    println!("verifying direct writes to follower are rejected");
    let reject_res = follower_client.put(PutRequest
    {
        key: b"illegal_key".to_vec(),
        value: b"illegal_val".to_vec(),
    }).await;
    println!("follower write rejected as expected: {}", reject_res.is_err());
    assert!(reject_res.is_err());

    leader_handle.abort();
    follower_handle.abort();
    Ok(())
}

async fn test_metrics_and_rehearsal_synchronization() -> Result<(), Box<dyn std::error::Error>>
{
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use server::proto::ebbinghaus_service_client::EbbinghausServiceClient;
    use server::proto::ebbinghaus_service_server::EbbinghausServiceServer;
    use server::proto::{GetRequest, PutRequest};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    println!("initializing leader and follower with metrics registry");

    let leader_metrics = Arc::new(metrics::MetricsRegistry::new());
    let leader_engine = Arc::new(Mutex::new(memtable::MemTable::new()));
    let leader_service = server::StorageServer::new_leader_with_metrics(leader_engine.clone(), leader_metrics.clone());

    let follower_metrics = Arc::new(metrics::MetricsRegistry::new());
    let follower_engine = Arc::new(Mutex::new(memtable::MemTable::new()));
    let follower_service = server::StorageServer::new_follower_with_metrics(follower_engine.clone(), follower_metrics.clone());

    // 1. bind follower on ephemeral port
    let follower_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let follower_addr = follower_listener.local_addr()?;
    let follower_incoming = tokio_stream::wrappers::TcpListenerStream::new(follower_listener);

    let follower_service_clone = follower_service.clone();
    let follower_handle = tokio::spawn(async move
    {
        let _ = tonic::transport::Server::builder()
            .add_service(EbbinghausServiceServer::new(follower_service_clone))
            .serve_with_incoming(follower_incoming)
            .await;
    });

    // 2. register follower with leader
    let follower_url = format!("http://{}", follower_addr);
    leader_service.add_follower(follower_url).await?;

    // 3. bind leader on ephemeral port
    let leader_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let leader_addr = leader_listener.local_addr()?;
    let leader_incoming = tokio_stream::wrappers::TcpListenerStream::new(leader_listener);

    let leader_service_clone = leader_service.clone();
    let leader_handle = tokio::spawn(async move
    {
        let _ = tonic::transport::Server::builder()
            .add_service(EbbinghausServiceServer::new(leader_service_clone))
            .serve_with_incoming(leader_incoming)
            .await;
    });

    // 4. start metrics http server on ephemeral port
    let metrics_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let metrics_addr = metrics_listener.local_addr()?;
    let leader_metrics_clone = leader_metrics.clone();
    let metrics_handle = tokio::spawn(async move
    {
        let _ = metrics::start_metrics_server(metrics_listener, leader_metrics_clone).await;
    });

    // 5. write key to leader
    let mut leader_client = EbbinghausServiceClient::connect(format!("http://{}", leader_addr)).await?;
    let put_res = leader_client.put(PutRequest
    {
        key: b"telemetry_key".to_vec(),
        value: b"telemetry_val".to_vec(),
    }).await?;
    assert!(put_res.into_inner().success);
    println!("wrote telemetry_key to leader");

    // wait for async replication to follower
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // 6. perform 3 reads on follower to buffer rehearsals
    let mut follower_client = EbbinghausServiceClient::connect(format!("http://{}", follower_addr)).await?;
    for _ in 0..3
    {
        let get_res = follower_client.get(GetRequest
        {
            key: b"telemetry_key".to_vec(),
        }).await?;
        assert!(get_res.into_inner().found);
    }
    println!("performed 3 reads on follower (buffered in follower rehearsal queue)");

    // 7. scrape prometheus /metrics endpoint over tcp
    let mut stream = tokio::net::TcpStream::connect(metrics_addr).await?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").await?;
    let mut resp = Vec::new();
    let mut buf = [0u8; 1024];
    loop
    {
        let n = stream.read(&mut buf).await?;
        if n == 0
        {
            break;
        }
        resp.extend_from_slice(&buf[..n]);
    }
    let body = String::from_utf8_lossy(&resp);
    println!("scraped /metrics endpoint ({} bytes)", body.len());
    assert!(body.contains("ebbinghaus_requests_total"));
    assert!(body.contains("ebbinghaus_request_duration_seconds"));
    assert!(body.contains("op=\"put\""));
    println!("verified prometheus metrics output contains request counters and latency histograms");

    // 8. verify leader access count before synchronization
    let count_before =
    {
        let engine = leader_engine.lock().await;
        engine.get_access_count(b"telemetry_key").unwrap_or(0)
    };
    println!("leader access count before sync: {}", count_before);
    assert_eq!(count_before, 1);

    // 9. sync follower rehearsals to leader
    println!("flushing rehearsal buffer from follower to leader");
    let sync_ack = follower_service.sync_rehearsals_to_leader(&format!("http://{}", leader_addr)).await?;
    assert!(sync_ack);

    // 10. verify leader access count after synchronization
    let count_after =
    {
        let engine = leader_engine.lock().await;
        engine.get_access_count(b"telemetry_key").unwrap_or(0)
    };
    println!("leader access count after rehearsal sync: {}", count_after);
    assert_eq!(count_after, 4);
    println!("rehearsal sync successfully eliminated decay skew between nodes");

    leader_handle.abort();
    follower_handle.abort();
    metrics_handle.abort();
    Ok(())
}
