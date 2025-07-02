use crossbeam_skiplist::{SkipMap};
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::{Instant, SystemTime, SystemTimeError, UNIX_EPOCH};

// Type aliases
pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

#[derive(Debug)]
pub struct Memtable {
    data: SkipMap<Key, Entry>,
    size_bytes: AtomicU64,
    sequence_counter: AtomicU64,
    created_at: Instant,
    frozen: AtomicBool
}

#[derive(Debug, Clone)]
pub struct Entry {
    value: Vec<u8>,
    sequence: u64,
    entry_type: EntryType,
    timestamp: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EntryType {
    PUT,
    DELETE,
}

#[derive(Debug, thiserror::Error)]
pub enum MemtableError {
    #[error("Memtable is frozen and cannot accept writes")]
    Frozen,
    #[error("Key is empty")]
    EmptyKey,
    #[error("Value too large: {size} bytes")]
    ValueTooLarge { size: usize },
}

impl Memtable {
    const MAX_VALUE_SIZE: usize = 1024 * 1024; // 1MB limit
    const ENTRY_OVERHEAD: usize = 32;      // est memory used per entry in addition to the value stored
   
    pub fn new() -> Memtable {
        return Memtable{data: SkipMap::new(), 
                size_bytes: AtomicU64::new(0),
                sequence_counter: AtomicU64::new(0),
                created_at: Instant::now(),
                frozen: AtomicBool::new(false)
                }
    }

    pub fn put(&self, key: Key, value: Vec<u8>) -> Result<(), MemtableError> {
        if key.is_empty() {
            return Err(MemtableError::EmptyKey);
        }

        if value.len() > Self::MAX_VALUE_SIZE {
            return Err(MemtableError::ValueTooLarge { size: (value.len()) })
        }

        if self.is_frozen()  {
            return Err(MemtableError::Frozen);
        }

        let seq_num = self.sequence_counter.fetch_add(1, Ordering::SeqCst);

        let timestamp = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs();

        let size = Self::ENTRY_OVERHEAD + value.len() + key.len();

        let entry = Entry{value: value,
                        sequence: seq_num,
                        entry_type: EntryType::PUT,
                        timestamp: timestamp
                        };

        self.size_bytes.fetch_add(size as u64, Ordering::SeqCst);
        self.data.insert(key, entry);

        return Ok(());
    }

    pub fn delete(&self, key: Key) -> Result<(), MemtableError> {
        if key.is_empty() {
            return Err(MemtableError::EmptyKey)
        }
        if self.is_frozen() {
            return Err(MemtableError::Frozen)
        }

        let timestamp = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs();
                    
        let seq_num = self.sequence_counter.fetch_add(1, Ordering::SeqCst);

        let entry = Entry{value: Vec::new(),
                                sequence: seq_num,
                                entry_type: EntryType::DELETE,
                                timestamp: timestamp};

        let val_size = self.data.get(&key).map(|entry_ref| entry.value.len()).unwrap_or_default();
        self.size_bytes.fetch_sub(val_size as u64, Ordering::Relaxed);
        self.data.insert(key, entry);
        return Ok(())
    }
    pub fn get(&self, key: &Key) -> Option<Entry> {
        return self.data.get(key).map(|entry_ref| entry_ref.value().clone());
    }
    
    pub fn range_scan<'a>(&'a self, start: &Key, end: &Key) -> impl Iterator<Item = (Key, Entry)> + 'a {
        self.data
            .range(start.clone()..=end.clone())
            .map(|entry_ref| (entry_ref.key().clone(), entry_ref.value().clone()))
    }

    pub fn size_bytes(&self) -> u64 {
        return self.size_bytes.load(Ordering::Relaxed);
    }
    pub fn freeze(&self) -> bool {
        self.frozen
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }
    pub fn is_frozen(&self) -> bool {
        return self.frozen.load(Ordering::Relaxed)
    }
    pub fn iter_all(&self) -> impl Iterator<Item = (Key, Entry)> + '_ {
        return self.data
                    .iter()
                    .map(|entry_ref| (entry_ref.key().clone(), entry_ref.value().clone()))
    }
    pub fn entry_count(&self) -> usize {
        return self.data.len();
    }
    pub fn age(&self) -> std::time::Duration {
        return self.created_at.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::sync::Arc;

    #[test]
    fn test_memtable_creation() {
        let tbl = Memtable::new();
        assert_eq!(tbl.size_bytes(), 0);
        assert_eq!(tbl.entry_count(), 0);
        assert!(!tbl.is_frozen());
    }

    #[test]
    fn test_put_and_get() {
        let tbl = Memtable::new();
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();

        assert!(tbl.put(key.clone(), value.clone()).is_ok());
        
        let retrieved = tbl.get(&key).unwrap();
        assert_eq!(retrieved.value, value);
        assert_eq!(retrieved.entry_type, EntryType::PUT);
        assert!(tbl.size_bytes() > 0);
    }

    #[test]
    fn test_delete() {
        let tbl = Memtable::new();
        let key = b"test_key".to_vec();

        assert!(tbl.delete(key.clone()).is_ok());
        
        let retrieved = tbl.get(&key).unwrap();
        assert_eq!(retrieved.entry_type, EntryType::DELETE);
        assert!(retrieved.value.is_empty());
    }

    #[test]
    fn test_freeze() {
        let tbl = Memtable::new();
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();

        // Should be able to write before freeze
        assert!(tbl.put(key.clone(), value.clone()).is_ok());
        
        // Freeze should succeed
        assert!(tbl.freeze());
        assert!(tbl.is_frozen());
        
        // Second freeze should fail
        assert!(!tbl.freeze());
        
        // Should not be able to write after freeze
        assert!(matches!(tbl.put(key, value), Err(MemtableError::Frozen)));
    }

    #[test]
    fn test_concurrent_writes() {
        let tbl = Arc::new(Memtable::new());
        let mut handles = vec![];

        // Spawn multiple threads writing concurrently
        for i in 0..10 {
            let tbl_clone = Arc::clone(&tbl);
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("key_{}_{}", i, j).into_bytes();
                    let value = format!("value_{}_{}", i, j).into_bytes();
                    tbl_clone.put(key, value).unwrap();
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Should have 1000 entries total
        assert_eq!(tbl.entry_count(), 1000);
        assert!(tbl.size_bytes() > 0);
    }

    #[test]
    fn test_range_scan() {
        let tbl = Memtable::new();
        
        // Insert some ordered data
        for i in 0..10 {
            let key = format!("key_{:02}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            tbl.put(key, value).unwrap();
        }

        let start_key = b"key_03".to_vec();
        let end_key = b"key_07".to_vec();
        
        let results: Vec<_> = tbl.range_scan(&start_key, &end_key).collect();
        
        // Should include keys 03, 04, 05, 06, 07 (inclusive range)
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].0, b"key_03".to_vec());
        assert_eq!(results[4].0, b"key_07".to_vec());
    }

    #[test]
    fn test_validation() {
        let tbl = Memtable::new();
        
        // Empty key should fail
        assert!(matches!(tbl.put(vec![], b"value".to_vec()), Err(MemtableError::EmptyKey)));
        
        // Very large value should fail
        let large_value = vec![0u8; Memtable::MAX_VALUE_SIZE + 1];
        assert!(matches!(
            tbl.put(b"key".to_vec(), large_value),
            Err(MemtableError::ValueTooLarge { .. })
        ));
    }

    #[test]
    fn test_sequence_numbers() {
        let tbl = Memtable::new();
        
        let key1 = b"key1".to_vec();
        let key2 = b"key2".to_vec();
        let value = b"value".to_vec();

        tbl.put(key1.clone(), value.clone()).unwrap();
        tbl.put(key2.clone(), value.clone()).unwrap();

        let entry1 = tbl.get(&key1).unwrap();
        let entry2 = tbl.get(&key2).unwrap();

        // Sequence numbers should be different and increasing
        assert_ne!(entry1.sequence, entry2.sequence);
        assert!(entry1.sequence < entry2.sequence);
    }
}