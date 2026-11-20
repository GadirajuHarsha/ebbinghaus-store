use std::path::Path;
use std::io::{Read, Write};

#[derive(Clone, Debug)]
pub struct BloomFilter
{
    bits: Vec<u8>,
    num_bits: usize,
    num_hashes: usize,
}

impl BloomFilter
{
    // create a new bloom filter with specified bit count and hash count
    pub fn new(num_bits: usize, num_hashes: usize) -> Self
    {
        let num_bytes = (num_bits + 7) / 8;
        BloomFilter
        {
            bits: vec![0u8; num_bytes],
            num_bits,
            num_hashes,
        }
    }

    // generate hash indices using double hashing (kirsch-mitzenmacher)
    fn get_hash_indices(&self, key: &[u8]) -> Vec<usize>
    {
        let mut h1 = crc32fast::Hasher::new_with_initial(0xbc9f1d34);
        h1.update(key);
        let hash1 = h1.finalize() as u64;

        let mut h2 = crc32fast::Hasher::new_with_initial(0x5a827999);
        h2.update(key);
        let hash2 = h2.finalize() as u64;

        let mut indices = Vec::with_capacity(self.num_hashes);
        for i in 0..self.num_hashes
        {
            let combined = hash1.wrapping_add((i as u64).wrapping_mul(hash2));
            indices.push((combined % (self.num_bits as u64)) as usize);
        }
        indices
    }

    // add a key to the bloom filter
    pub fn insert(&mut self, key: &[u8])
    {
        for index in self.get_hash_indices(key)
        {
            let byte_idx = index / 8;
            let bit_idx = index % 8;
            self.bits[byte_idx] |= 1 << bit_idx;
        }
    }

    // check if a key might be in the set (false means definitely not)
    pub fn contains(&self, key: &[u8]) -> bool
    {
        for index in self.get_hash_indices(key)
        {
            let byte_idx = index / 8;
            let bit_idx = index % 8;
            if (self.bits[byte_idx] & (1 << bit_idx)) == 0
            {
                return false;
            }
        }
        true
    }

    // save the bloom filter to disk
    pub fn save_to_file(&self, path: &Path) -> std::io::Result<()>
    {
        let mut file = std::fs::File::create(path)?;
        file.write_all(&(self.num_bits as u32).to_le_bytes())?;
        file.write_all(&(self.num_hashes as u32).to_le_bytes())?;
        file.write_all(&self.bits)?;
        file.sync_all()?;
        Ok(())
    }

    // load a bloom filter from disk
    pub fn load_from_file(path: &Path) -> std::io::Result<Self>
    {
        let mut file = std::fs::File::open(path)?;
        let mut header = [0u8; 8];
        file.read_exact(&mut header)?;

        let num_bits = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
        let num_hashes = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;

        let mut bits = Vec::new();
        file.read_to_end(&mut bits)?;

        Ok(BloomFilter
        {
            bits,
            num_bits,
            num_hashes,
        })
    }
}

#[cfg(test)]
mod tests
{
    use super::*;

    #[test]
    fn test_bloom_insert_and_contains()
    {
        let mut filter = BloomFilter::new(512, 3);
        filter.insert(b"test_key_1");
        filter.insert(b"test_key_2");

        assert!(filter.contains(b"test_key_1"));
        assert!(filter.contains(b"test_key_2"));
        assert!(!filter.contains(b"non_existent_key"));
    }

    #[test]
    fn test_bloom_save_and_load()
    {
        let mut filter = BloomFilter::new(512, 3);
        filter.insert(b"saved_key");

        let test_path = std::path::Path::new("test_filter.bloom");
        filter.save_to_file(test_path).unwrap();

        let loaded = BloomFilter::load_from_file(test_path).unwrap();
        assert!(loaded.contains(b"saved_key"));
        assert!(!loaded.contains(b"absent_key"));

        let _ = std::fs::remove_file(test_path);
    }
}
