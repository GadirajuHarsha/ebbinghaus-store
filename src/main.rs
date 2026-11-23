mod wal;
mod memtable;
mod bloom;
mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>
{
    // check if running as a persistent grpc server daemon
    let args: Vec<String> = std::env::args().collect();
    if args.contains(&"--server".to_string())
    {
        let port = 50051;
        let addr = format!("127.0.0.1:{}", port).parse()?;
        let engine = std::sync::Arc::new(tokio::sync::Mutex::new(memtable::MemTable::new()));
        let service = server::StorageServer::new(engine);

        println!("ebbinghaus storage engine running on {}", addr);
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
