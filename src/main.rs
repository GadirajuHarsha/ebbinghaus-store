mod wal;
mod memtable;

fn main() -> std::io::Result<()>
{
    println!("--- running basic lifecycle test ---");
    test_basic_lifecycle()?;

    println!("\n--- running sstable flush test ---");
    test_sstable_flush()?;

    Ok(())
}

// verifies that put, get, and delete (tombstone insertion) work in memory
fn test_basic_lifecycle() -> std::io::Result<()>
{
    let mut table = memtable::MemTable::new();
    let key = b"lifecycle_key";
    let value = b"lifecycle_value";

    // put
    table.put(wal::RecordType::Put, key, value)?;
    if let Some(res) = table.get(key)
    {
        println!("initial put found: {:?}", res);
    }

    // delete
    table.delete(key)?;
    if table.get(key).is_none()
    {
        println!("key successfully deleted (tombstone works)");
    }

    // put again
    table.put(wal::RecordType::Put, key, value)?;
    if let Some(res) = table.get(key)
    {
        println!("second put found: {:?}", res);
    }

    Ok(())
}

// verifies that the memtable automatically flushes to 0.sst when full
fn test_sstable_flush() -> std::io::Result<()>
{
    // this test assumes you set max_size to 60 in memtable.rs
    let mut table = memtable::MemTable::new();
    
    let key1 = b"large_key_one_for_testing"; // ~25 bytes
    let val1 = b"large_value_one_for_testing"; // ~27 bytes
    // total so far: ~52 bytes (under 60)

    let key2 = b"trigger_key";
    let val2 = b"trigger_val";
    // this second write should push us over 60 bytes and trigger the flush

    println!("writing first large record");
    table.put(wal::RecordType::Put, key1, val1)?;
    
    println!("writing second record (triggering 0.sst creation)");
    table.put(wal::RecordType::Put, key2, val2)?;

    println!("check project folder for '0.sst'");
    
    Ok(())
}
