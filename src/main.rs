mod wal;
mod memtable;
mod bloom;

fn main() -> std::io::Result<()>
{
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

    println!("\n--- 7. cognitive compaction test ---");
    test_cognitive_compaction()?;

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

fn test_cognitive_compaction() -> std::io::Result<()>
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
