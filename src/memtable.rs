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
            max_size: 60,
        };
    }

    // helper to get current timestamp
    fn get_timestamp(&self) -> u64
    {
        return std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis()
            .try_into()
            .unwrap();
    }

    // helper to calculate retention strength of a single record
    fn retention_strength(&mut self, timestamp: u64, access_count: u32) -> f64
    {
        let forgetting_constant = 2500;
        let s = (forgetting_constant * access_count).max(1) as f64;
        let t = (self.get_timestamp() - timestamp) as f64;
        let exponent = -(t / s);
        return exponent.exp();
    }

    // calculate retention strength of all records
    pub fn calculate_scores(&mut self) -> std::io::Result<Vec<(Vec<u8>, f64)>>
    {
        let ssts = std::fs::read_dir("sst")?;
        // create list of scores (key-value pairs)
        let mut scores = Vec::new();
        for sst in ssts
        {
            let sst = sst?;
            let path = sst.path();
            let mut table = std::fs::File::open(path)?;
            
            use std::io::Read;
            use std::io::ErrorKind;

            loop
            {
                let mut header = [0u8; 32]; // 32 byte buffer to store the header
                match table.read_exact(&mut header)
                {
                    Ok(_) =>
                    {
                        // convert the necessary slices into 4 byte arrays
                        let key_array: [u8; 4] = header[4..8].try_into().expect("Key length error");
                        let key_length = u32::from_le_bytes(key_array);
                        let val_array: [u8; 4] = header[8..12].try_into().expect("Value length error");
                        let val_length = u32::from_le_bytes(val_array);
                        let access_array: [u8; 4] = header[12..16].try_into().expect("Access count slice was wrong length");
                        let access_count = u32::from_le_bytes(access_array);
                        let timestamp_array: [u8; 8] = header[16..24].try_into().expect("Timestamp slice was wrong length");
                        let timestamp = u64::from_le_bytes(timestamp_array);

                        // read the payload
                        let mut key_buffer = vec![0u8; key_length as usize];
                        table.read_exact(&mut key_buffer)?;
                        let mut value_buffer = vec![0u8; val_length as usize];
                        table.read_exact(&mut value_buffer)?;

                        // add the key and score to the output vector
                        scores.push((key_buffer, self.retention_strength(timestamp, access_count)));
                    },
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof =>
                    {
                        // reached end of file
                        break;
                    },
                    Err(e) =>
                    {
                        // any other kind of error
                        return Err(e);
                    },
                }
            }
        }
        Ok(scores)
    }

    // public abstraction: add a brand new record
    pub fn put(&mut self, record_type: crate::wal::RecordType, key: &[u8], value: &[u8]) -> std::io::Result<()>
    {
        self.put_backend(record_type, key, value, 1)
    }

    // private: add a record, but can specify access count so far
    fn put_backend(&mut self, record_type: crate::wal::RecordType, key: &[u8], value: &[u8], access_count: u32) -> std::io::Result<()>
    {
        crate::wal::append_to_wal(record_type, key, value)?;
        if self.current_size + key.len() + value.len() > self.max_size
        {    
            // convert the current memtable into an sstable
            self.create_sstable()?;
            
            // flush this memtable
            self.map.clear();
            self.current_size = 0;
        }
        let timestamp = self.get_timestamp();
        self.map.insert(key.to_vec(), (value.to_vec(), record_type, timestamp, access_count));
        self.current_size += key.len() + value.len();
        Ok(())
    }

    // access a record
    pub fn get(&mut self, key: &[u8]) -> Option<&Vec<u8>>
    {
        let timestamp = self.get_timestamp();
        if self.map.contains_key(key)
        {
            let record = self.map.get_mut(key).unwrap();
            if record.1 == crate::wal::RecordType::Delete
            {
                return None;
            }
            record.2 = timestamp;
            record.3 += 1;
            return Some(&record.0)
        }
        return self.search_sstables(key);
    }

    // insert a tombstone record (representing a key deletion)
    pub fn delete(&mut self, key: &[u8]) -> std::io::Result<()>
    {
        self.put(crate::wal::RecordType::Delete, key, &[])
    }

    // offload the memtable into an sstable
    pub fn create_sstable(&self) -> std::io::Result<()>
    {
        use std::fs::OpenOptions;
        use std::io::Write;
        
        // open the sstable file (and create the directory if it doesnt exist)
        std::fs::create_dir_all("sst")?;
        let mut sst = OpenOptions::new()
            .append(true)
            .create(true)
            .write(true)
            .open(format!("sst/{}.sst", self.get_timestamp()))?;
        
        for (key, (value, record_type, timestamp, access_count)) in self.map.iter()
        {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(key);
            hasher.update(value);
            let cs = hasher.finalize();

            let header = crate::wal::RecordHeader
            {
                checksum: cs,
                key_length: key.len() as u32,
                value_length: value.len() as u32,
                access_count: *access_count,
                timestamp: *timestamp,
                record_type: *record_type,
                padding: [0; 7],
            };

            sst.write_all(&header.checksum.to_le_bytes())?;
            sst.write_all(&header.key_length.to_le_bytes())?;
            sst.write_all(&header.value_length.to_le_bytes())?;
            sst.write_all(&header.access_count.to_le_bytes())?;
            sst.write_all(&header.timestamp.to_le_bytes())?;
            sst.write_all(&(header.record_type as u8).to_le_bytes())?;
            sst.write_all(&header.padding)?;
            sst.write_all(key)?;
            sst.write_all(value)?;
        }
        sst.sync_all()?;
        Ok(())
    }

    // helper to iterate through sstables to find a key
    fn search_sstables(&mut self, key: &[u8]) -> Option<&Vec<u8>>
    {
        use std::fs;
        use std::time::SystemTime;
        
        // sort ssts by timestamp in reverse chronological order
        let mut ssts: Vec<(SystemTime, fs::DirEntry)> = fs::read_dir("sst")
            .ok()?
            .into_iter()
            .filter_map(|entry|
                {
                    let entry = entry.ok()?;
                    let modified = entry.metadata().ok()?.modified().ok()?;
                    Some((modified, entry))
                })
            .collect();
        ssts.sort_by(|a, b| b.0.cmp(&a.0));

        for (_time, sst) in ssts
        {
            let mut table = std::fs::File::open(sst.path()).ok()?;
            
            use std::io::Read;
            use std::io::ErrorKind;

            loop
            {
                let mut header = [0u8; 32]; // 32 byte buffer to store the header
                match table.read_exact(&mut header)
                {
                    Ok(_) =>
                    {
                        // get the key from the header
                        let key_array: [u8; 4] = header[4..8].try_into().expect("Key slice was wrong length");
                        let key_length = u32::from_le_bytes(key_array);
                        let mut key_buffer = vec![0u8; key_length as usize];
                        table.read_exact(&mut key_buffer).ok()?;

                        // get the value
                        let value_array: [u8; 4] = header[8..12].try_into().expect("Value slice was wrong length");
                        let value_length = u32::from_le_bytes(value_array);
                        let mut value_buffer = vec![0u8; value_length as usize];
                        table.read_exact(&mut value_buffer).ok()?;

                        if key_buffer == key
                        {
                            // get the record_type and handle each case separately
                            let type_bytes = &header[24..25];
                            let type_array: [u8; 1] = type_bytes.try_into().expect("Record type slice was wrong length");
                            let record_type = u8::from_le_bytes(type_array);
                            if record_type == (crate::wal::RecordType::Delete as u8)
                            {
                                return None;
                            }
                            else
                            {
                                // get the checksum
                                let checksum_bytes = &header[0..4];
                                let checksum_array: [u8; 4] = checksum_bytes.try_into().expect("Checksum slice was wrong length");
                                let checksum = u32::from_le_bytes(checksum_array);

                                // check the key and value against the checksum
                                let mut hasher = crc32fast::Hasher::new();
                                hasher.update(&key_buffer);
                                hasher.update(&value_buffer);
                                let cs = hasher.finalize();
                                if cs != checksum
                                {
                                    continue;
                                }

                                // get the access count
                                let access_array: [u8; 4] = header[12..16].try_into().expect("Access count slice was wrong length");
                                let access_count = u32::from_le_bytes(access_array);

                                // saving to variable so that we can ignore any Result for now
                                let _ = self.put_backend(crate::wal::RecordType::Put, &key_buffer, &value_buffer, access_count + 1);
                                return self.map.get(key).map(|record| &record.0);
                            }
                        }
                    },
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof =>
                    {
                        // reached end of file
                        break;
                    },
                    Err(_e) =>
                    {
                        // any other error type
                        continue;
                    },
                }
            }
        }
        None
    }
}