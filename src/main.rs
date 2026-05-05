mod wal;
mod memtable;

fn main() -> std::io::Result<()> {
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

    // pause before calculating scores to see the difference between aging memories
    std::thread::sleep(std::time::Duration::from_millis(500));

    let scores = table.calculate_scores()?;
    for (key, score) in scores
    {
        println!("    - key: {:?}, retention strength: {:.4}", String::from_utf8_lossy(&key), score);
    }
    Ok(())
}
