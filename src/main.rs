mod wal;
mod memtable;

fn main() -> std::io::Result<()>
{
    let mut table = memtable::MemTable::new();

    let key = b"test_key";
    let value = b"test_value";

    table.put(wal::RecordType::Put, key, value)?;

    if let Some(retrieved) = table.get(key)
    {
        println!("Found: {:?}", retrieved);
    }

    table.delete(key)?;

    if let Some(retrieved) = table.get(key)
    {
        println!("Found: {:?}", retrieved);
    }
    else
    {
        println!("Key not found");
    }

    table.put(wal::RecordType::Put, key, value)?;

    if let Some(retrieved) = table.get(key)
    {
        println!("Found: {:?}", retrieved);
    }

    Ok(())
}
