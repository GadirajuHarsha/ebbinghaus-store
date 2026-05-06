// store WAL in a binary format
// consecutive sequence of records

#[derive(Debug, Clone, Copy, PartialEq)]  // stop compiler from complaining
#[repr(u8)] // force enum to be exactly 1 byte
pub enum RecordType
{
    Put = 0,
    Delete = 1,
}

#[derive(Debug, Clone)]  // stop compiler from complaining
#[repr(C, align(8))]           // prevent the compiler from reordering the fields, 8 byte align
pub struct RecordHeader
{
    pub checksum: u32,             // CRC32 hash to detect if the data was corrupted
    pub key_length: u32,           // byte length of the key
    pub value_length: u32,         // byte length of the value
    pub access_count: u32,         // number of times this record was read
    pub timestamp: u64,            // timestamp of this entry for "forgetting" policy
    pub record_type: RecordType,   // PUT (insert) or DELETE
    pub padding: [u8; 7],          // 7 bytes of padding for 8 byte alignment
}
// payload is written right after header; variable size, contains actual key and value bytes

pub fn append_to_wal(record_type: RecordType, key: &[u8], value: &[u8]) -> std::io::Result<()>
{
    use std::fs::OpenOptions;
    use std::io::Write;

    // open the WAL file for appends by enabling write (and creating if it doesnt exist)
    let mut wal = OpenOptions::new()
        .append(true)
        .create(true)
        .write(true)
        .open("wal.log")?;

    // create a hash of the key and value to use as a checksum
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(key);
    hasher.update(value);
    let cs = hasher.finalize();

    let header = RecordHeader
    {
        checksum: cs,
        key_length: key.len() as u32,
        value_length: value.len() as u32,
        access_count: 1,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis()
            .try_into()
            .unwrap(),
        record_type: record_type,
        padding: [0; 7],
    };

    wal.write_all(&header.checksum.to_le_bytes())?;
    wal.write_all(&header.key_length.to_le_bytes())?;
    wal.write_all(&header.value_length.to_le_bytes())?;
    wal.write_all(&header.access_count.to_le_bytes())?;
    wal.write_all(&header.timestamp.to_le_bytes())?;
    wal.write_all(&(header.record_type as u8).to_le_bytes())?;
    wal.write_all(&header.padding)?;
    wal.write_all(key)?;
    wal.write_all(value)?;
    wal.sync_all()
}

pub fn recover_from_wal() -> std::io::Result<Vec<(Vec<u8>, Vec<u8>, u8)>>
{
    use std::io::Read;
    use std::io::ErrorKind;

    // create output list of records (key-value pairs)
    let mut records = Vec::new();

    // open the WAL file for read
    let mut wal = std::fs::File::open("wal.log")?;
    
    loop
    {
        let mut header = [0u8; 32]; // 32 byte buffer to store the header
        match wal.read_exact(&mut header)
        {
            Ok(_) =>
            {
                // convert the necessary slices into 4 byte arrays
                let key_array: [u8; 4] = header[4..8].try_into().expect("Key slice was wrong length");
                let key_length = u32::from_le_bytes(key_array);
                let value_array: [u8; 4] = header[8..12].try_into().expect("Value slice was wrong length");
                let value_length = u32::from_le_bytes(value_array);
                
                // read the payload (which uses vec because its a dynamic size)
                let mut key_buffer = vec![0u8; key_length as usize];
                wal.read_exact(&mut key_buffer)?;
                let mut value_buffer = vec![0u8; value_length as usize];
                wal.read_exact(&mut value_buffer)?;

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
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Checksum mismatch"));
                }

                // get the record_type
                let type_bytes = &header[24..25];
                let type_array: [u8; 1] = type_bytes.try_into().expect("Record type slice was wrong length");
                let record_type = u8::from_le_bytes(type_array);

                // add the key and value (and record_type) tuple to the output vector
                records.push((key_buffer, value_buffer, record_type));
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
    Ok(records)
}