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
    pub timestamp: u64,            // timestamp of this entry for "forgetting" policy
    pub record_type: RecordType,   // PUT (insert) or DELETE
    pub padding: [u8; 3],          // 3 bytes of padding for 8 byte alignment
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
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs(),
        record_type: record_type,
        padding: [0; 3],
    };

    wal.write_all(&header.checksum.to_le_bytes())?;
    wal.write_all(&header.key_length.to_le_bytes())?;
    wal.write_all(&header.value_length.to_le_bytes())?;
    wal.write_all(&header.timestamp.to_le_bytes())?;
    wal.write_all(&(header.record_type as u8).to_le_bytes())?;
    wal.write_all(&header.padding)?;
    wal.write_all(key)?;
    wal.write_all(value)?;
    wal.sync_all()
}

pub fn recover_from_wal() -> std::io::Result<Vec<(Vec<u8>, Vec<u8>)>>
{
    use std::io::Read;
    use std::io::ErrorKind;

    // create output list of records (key-value pairs)
    let mut records = Vec::new();

    // open the WAL file for read
    let mut wal = std::fs::File::open("wal.log")?;
    
    loop
    {
        let mut header = [0u8; 24]; // 24 byte buffer to store the header
        match wal.read_exact(&mut header)
        {
            Ok(_) =>
            {
                // get the necessary slices from the buffer
                let key_length_bytes = &header[4..8];
                let value_length_bytes = &header[8..12];
                let checksum_bytes = &header[0..4];

                // convert the slices into 4 byte arrays
                let key_array: [u8; 4] = key_length_bytes.try_into().expect("Key slice was wrong length");
                let key_length = u32::from_le_bytes(key_array);
                let value_array: [u8; 4] = value_length_bytes.try_into().expect("Value slice was wrong length");
                let value_length = u32::from_le_bytes(value_array);
                let checksum_array: [u8; 4] = checksum_bytes.try_into().expect("Checksum slice was wrong length");
                let checksum = u32::from_le_bytes(checksum_array);

                // read the payload (which uses vec because its a dynamic size)
                let mut key_buffer = vec![0u8; key_length as usize];
                wal.read_exact(&mut key_buffer)?;
                let mut value_buffer = vec![0u8; value_length as usize];
                wal.read_exact(&mut value_buffer)?;

                // check the key and value against the checksum
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&key_buffer);
                hasher.update(&value_buffer);
                let cs = hasher.finalize();
                if cs != checksum
                {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Checksum mismatch"));
                }

                // add the key and value tuple to the output vector
                records.push((key_buffer, value_buffer));
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