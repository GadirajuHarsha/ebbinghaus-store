use std::collections::BTreeMap;

pub struct MemTable
{
    // wrap a btree as the backing sorted list
    // key: tuple(value, record_type, timestamp, access_count)
    map: BTreeMap<Vec<u8>, (Vec<u8>, crate::wal::RecordType, u64, u32)>,   
    current_size: usize,
    max_size: usize,
}

impl MemTable // define memtable struct functions
{
    // create a new memtable
    pub fn new() -> Self
    {
        return MemTable
        {
            map: BTreeMap::new(),
            current_size: 0,
            max_size: 64,
        };
    }

    // helper to get current timestamp
    fn get_timestamp(&self) -> u64
    {
        return std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
    }

    // add a new record
    pub fn put(&mut self, record_type: crate::wal::RecordType, key: &[u8], value: &[u8]) -> std::io::Result<()>
    {
        crate::wal::append_to_wal(record_type, key, value)?;
        if self.current_size + key.len() + value.len() > self.max_size
        {    
            // convert the current memtable into an sstable
            self.create_sstable();
            
            // flush this memtable
            self.map.clear();
            self.current_size = 0;
        }
        let timestamp = self.get_timestamp();
        self.map.insert(key.to_vec(), (value.to_vec(), record_type, timestamp, 1));
        self.current_size += key.len() + value.len();
        Ok(())
    }

    // access a record
    pub fn get(&mut self, key: &[u8]) -> Option<&Vec<u8>>
    {
        let timestamp = self.get_timestamp();
        if let Some(record) = self.map.get_mut(key)
        {
            if record.1 == crate::wal::RecordType::Delete
            {
                return None;
            }
            record.2 = timestamp;
            record.3 += 1;
            return Some(&record.0)
        }
        None
    }

    pub fn delete(&mut self, key: &[u8]) -> std::io::Result<()>
    {
        self.put(crate::wal::RecordType::Delete, key, &[])
    }

    pub fn create_sstable(&self)
    {
        // placeholder: create sstable of this memtable
    }
}