use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::PathBuf;

pub struct MemTable
{
    // wrap a btree as the backing sorted list
    // key: tuple(value, record_type, timestamp, access_count)
    map: BTreeMap<Vec<u8>, (Vec<u8>, crate::wal::RecordType, u64, u32)>,   
    current_size: usize,
    max_size: usize,
    bloom_filters: HashMap<PathBuf, crate::bloom::BloomFilter>,
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
            bloom_filters: HashMap::new(),
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
        // create list of scores (key-value pairs)
        let mut scores = Vec::new();
        let tiers = ["sst/tier0", "sst/tier1", "sst/tier2"];
        for tier in tiers
        {
            let ssts = std::fs::read_dir(tier)?;
            for sst in ssts
            {
                let sst = sst?;
                let path = sst.path();
                if path.extension().and_then(|s| s.to_str()) != Some("sst")
                {
                    continue;
                }
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

            let tier0count = std::fs::read_dir("sst/tier0").map(|d| d.count()).unwrap_or(0);
            if tier0count > 3
            {
                let _ = self.compact_tier(0);
            }
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
    pub fn create_sstable(&mut self) -> std::io::Result<()>
    {
        use std::fs::OpenOptions;
        use std::io::Write;
        
        // open the sstable file (and create the directory if it doesnt exist)
        std::fs::create_dir_all("sst")?;
        let tiers = ["sst/tier0", "sst/tier1", "sst/tier2"];
        for tier in tiers
        {
            std::fs::create_dir_all(tier)?;
        }

        let timestamp = self.get_timestamp();
        let sst_path = format!("sst/tier0/{}.sst", timestamp);
        let bloom_path = format!("sst/tier0/{}.bloom", timestamp);

        let mut sst = OpenOptions::new()
            .append(true)
            .create(true)
            .write(true)
            .open(&sst_path)?;

        let mut bloom = crate::bloom::BloomFilter::new(1024, 4);
        
        for (key, (value, record_type, timestamp, access_count)) in self.map.iter()
        {
            bloom.insert(key);

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
        bloom.save_to_file(std::path::Path::new(&bloom_path))?;
        self.bloom_filters.insert(PathBuf::from(&sst_path), bloom);
        Ok(())
    }

    // helper to iterate through sstables to find a key
    fn search_sstables(&mut self, key: &[u8]) -> Option<&Vec<u8>>
    {
        use std::fs;
        use std::time::SystemTime;
        
        // go through each storage tier
        let tiers = ["sst/tier0", "sst/tier1", "sst/tier2"];
        for tier in tiers
        {
            std::fs::create_dir_all(tier).ok()?;
            // sort ssts by timestamp in reverse chronological order
            let mut ssts: Vec<(SystemTime, fs::DirEntry)> = fs::read_dir(tier)
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
                let path = sst.path();
                if path.extension().and_then(|s| s.to_str()) != Some("sst")
                {
                    continue;
                }

                // check bloom filter to avoid unnecessary disk reads
                let mut maybe_present = true;
                if let Some(f) = self.bloom_filters.get(&path)
                {
                    if !f.contains(key)
                    {
                        maybe_present = false;
                    }
                }
                else
                {
                    let bloom_path = path.with_extension("bloom");
                    if let Ok(f) = crate::bloom::BloomFilter::load_from_file(&bloom_path)
                    {
                        if !f.contains(key)
                        {
                            maybe_present = false;
                        }
                        self.bloom_filters.insert(path.clone(), f);
                    }
                }

                if !maybe_present
                {
                    // key is guaranteed not to exist in this file
                    continue;
                }

                let mut table = std::fs::File::open(&path).ok()?;
                
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
        }
        None
    }

    // clear this tier by merging all of its sstables and dropping them down
    pub fn compact_tier(&mut self, tier: u8) -> std::io::Result<()>
    {
        // struct to help manage current position in each sstable file
        struct Scanner
        {
            file: std::io::BufReader<std::fs::File>,        
            timestamp: u64,
            access_count: u32,
            record_type: crate::wal::RecordType,
            is_done: bool,
            key: Vec<u8>,
            value: Vec<u8>,
        }

        impl Scanner
        {            
            fn new(file: std::fs::File) -> Option<Self>
            {
                let mut scanner = Scanner
                {
                    file: std::io::BufReader::new(file),
                    timestamp: 0,
                    access_count: 0,
                    record_type: crate::wal::RecordType::Put,
                    is_done: false,
                    key: Vec::new(),
                    value: Vec::new(),
                };
                scanner.next();
                if scanner.is_done
                {
                    None
                }
                else
                {
                    Some(scanner)
                }
            }
            
            // used to increment to the next header
            fn next(&mut self)
            {
                
                use std::io::Read;
                use std::io::ErrorKind;
                
                let mut is_valid = false;
                while !is_valid
                {
                    let mut header = [0u8; 32]; // 32 byte buffer to store the header
                    match self.file.read_exact(&mut header)
                    {
                        Ok(_) =>
                        {
                            // get the key from the header
                            let key_array: [u8; 4] = header[4..8].try_into().expect("Key slice was wrong length");
                            let key_length = u32::from_le_bytes(key_array);
                            let mut key_buffer = vec![0u8; key_length as usize];
                            self.file.read_exact(&mut key_buffer).expect("Error reading key");
                            
                            // get the value
                            let value_array: [u8; 4] = header[8..12].try_into().expect("Value slice was wrong length");
                            let value_length = u32::from_le_bytes(value_array);
                            let mut value_buffer = vec![0u8; value_length as usize];
                            self.file.read_exact(&mut value_buffer).expect("Error reading value");
                            
                            // get the checksum
                            let checksum_bytes = &header[0..4];
                            let checksum_array: [u8; 4] = checksum_bytes.try_into().expect("Checksum slice was wrong length");
                            let checksum = u32::from_le_bytes(checksum_array);

                            // check the key and value against the checksum
                            let mut hasher = crc32fast::Hasher::new();
                            hasher.update(&key_buffer);
                            hasher.update(&value_buffer);
                            let cs = hasher.finalize();
                            if cs == checksum
                            {
                                is_valid = true;
                                self.key = key_buffer as Vec<u8>;
                                self.value = value_buffer as Vec<u8>;
                            }
                            else
                            {
                                continue;
                            }

                            // get the record_type
                            let type_bytes = &header[24..25];
                            let type_array: [u8; 1] = type_bytes.try_into().expect("Record type slice was wrong length");
                            let type_val = u8::from_le_bytes(type_array);
                            self.record_type = if type_val == 0 
                            {
                                crate::wal::RecordType::Put
                            } 
                            else 
                            {
                                crate::wal::RecordType::Delete
                            };

                            // get the access count
                            let access_array: [u8; 4] = header[12..16].try_into().expect("Access count slice was wrong length");
                            self.access_count = u32::from_le_bytes(access_array);

                            // get the timestamp
                            let timestamp_array: [u8; 8] = header[16..24].try_into().expect("Timestamp slice was wrong length");
                            self.timestamp = u64::from_le_bytes(timestamp_array);
                        },
                        Err(e) if e.kind() == ErrorKind::UnexpectedEof =>
                        {
                            // reached end of file
                            self.is_done = true;
                            return;
                        },
                        Err(_e) =>
                        {
                            // any other error type
                            continue;
                        },
                    }
                }
            }
        }

        use std::io::Write;

        // determine source and target tiers
        let source_dir = format!("sst/tier{}", tier);
        let dest_tier = if tier < 2
        {
            tier + 1
        }
        else
        {
            2
        };
        let dest_dir = format!("sst/tier{}", dest_tier);
        std::fs::create_dir_all(&dest_dir)?;

        // gather all sstable file paths in this tier
        let mut file_paths = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&source_dir)
        {
            for entry in entries
            {
                if let Ok(entry) = entry
                {
                    let path = entry.path();
                    if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("sst")
                    {
                        file_paths.push(path);
                    }
                }
            }
        }

        // if there are no files to compact exit early
        if file_paths.is_empty()
        {
            return Ok(());
        }

        // initialize active scanners for each file
        let mut scanners = Vec::new();
        for path in &file_paths
        {
            let file = std::fs::File::open(path)?;
            if let Some(scanner) = Scanner::new(file)
            {
                scanners.push(scanner);
            }
        }

        // prepare the new merged sstable in the destination tier
        let timestamp = self.get_timestamp();
        let dest_path = format!("sst/tier{}/{}.sst", dest_tier, timestamp);
        let bloom_dest_path = format!("sst/tier{}/{}.bloom", dest_tier, timestamp);
        let mut dest_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&dest_path)?;

        let mut bloom = crate::bloom::BloomFilter::new(1024, 4);
        let mut records_written = 0;

        // k-way merge across all scanners
        while !scanners.is_empty()
        {
            // find the lexographically smallest key
            let mut min_key = scanners[0].key.clone();
            for scanner in &scanners[1..]
            {
                if scanner.key < min_key
                {
                    min_key = scanner.key.clone();
                }
            }

            // find the newest record among duplicates sharing this smallest key
            let mut newest_index = 0;
            let mut max_timestamp = 0;
            for (i, scanner) in scanners.iter().enumerate()
            {
                if scanner.key == min_key
                {
                    if scanner.timestamp >= max_timestamp
                    {
                        max_timestamp = scanner.timestamp;
                        newest_index = i;
                    }
                }
            }

            // clone winning record details
            let winner_key = scanners[newest_index].key.clone();
            let winner_val = scanners[newest_index].value.clone();
            let winner_type = scanners[newest_index].record_type;
            let winner_ts = scanners[newest_index].timestamp;
            let winner_access = scanners[newest_index].access_count;

            // advance every scanner that has this key to discard older duplicate versions
            for scanner in scanners.iter_mut()
            {
                if scanner.key == min_key
                {
                    scanner.next();
                }
            }

            // decide whether this record should be saved
            let is_bottom_tier = tier >= 2;
            let mut should_write = true;

            if is_bottom_tier
            {
                // drop tombstones permanently in the bottom tier
                if winner_type == crate::wal::RecordType::Delete
                {
                    should_write = false;
                }
                // drop forgotten records below retention threshold
                else if self.retention_strength(winner_ts, winner_access) < 0.10
                {
                    should_write = false;
                }
            }

            if should_write
            {
                bloom.insert(&winner_key);

                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&winner_key);
                hasher.update(&winner_val);
                let cs = hasher.finalize();

                let header = crate::wal::RecordHeader
                {
                    checksum: cs,
                    key_length: winner_key.len() as u32,
                    value_length: winner_val.len() as u32,
                    access_count: winner_access,
                    timestamp: winner_ts,
                    record_type: winner_type,
                    padding: [0; 7],
                };

                dest_file.write_all(&header.checksum.to_le_bytes())?;
                dest_file.write_all(&header.key_length.to_le_bytes())?;
                dest_file.write_all(&header.value_length.to_le_bytes())?;
                dest_file.write_all(&header.access_count.to_le_bytes())?;
                dest_file.write_all(&header.timestamp.to_le_bytes())?;
                dest_file.write_all(&(header.record_type as u8).to_le_bytes())?;
                dest_file.write_all(&header.padding)?;
                dest_file.write_all(&winner_key)?;
                dest_file.write_all(&winner_val)?;

                records_written += 1;
            }

            // remove scanners that have reached end of file
            scanners.retain(|s| !s.is_done);
        }

        dest_file.sync_all()?;

        // if no records survived compaction remove the empty destination file
        if records_written == 0
        {
            let _ = std::fs::remove_file(&dest_path);
        }
        else
        {
            bloom.save_to_file(std::path::Path::new(&bloom_dest_path))?;
            self.bloom_filters.insert(PathBuf::from(&dest_path), bloom);
        }

        // delete the old sstables that were merged
        for path in file_paths
        {
            if path != std::path::Path::new(&dest_path)
            {
                let _ = std::fs::remove_file(&path);
                let _ = std::fs::remove_file(path.with_extension("bloom"));
                self.bloom_filters.remove(&path);
            }
        }

        // ripple compaction down to the next tier if it has reached capacity
        if dest_tier == 1
        {
            let tier1_count = std::fs::read_dir("sst/tier1").map(|d| d.count()).unwrap_or(0);
            if tier1_count > 3
            {
                let _ = self.compact_tier(1);
            }
        }
        else if dest_tier == 2
        {
            let tier2_count = std::fs::read_dir("sst/tier2").map(|d| d.count()).unwrap_or(0);
            if tier2_count > 3
            {
                let _ = self.compact_tier(2);
            }
        }

        Ok(())
    }
}