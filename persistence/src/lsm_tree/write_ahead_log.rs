use bincode::enc::Encode;
use bincode::de::Decode;
use bincode::error::{EncodeError, DecodeError};
use std::fs;
use std::io::{self, BufWriter, Read, Take, Write};
use std::{fs::File};
use std::sync::atomic::AtomicU64;
use std::path::Path;
use crate::lsm_tree::memtable::{Entry, Key};

const CRC_ALG: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_CKSUM);
const MAX_ENTRY_SIZE: u32 = 1024 * 1024;

pub struct WAL {
    wal_folder: String,
    log_file_prefix: String,
    sequential_counter: AtomicU64,
    current_file_id: u64,
    current_writer: Option<BufWriter<File>>,
}

#[derive(Debug, thiserror::Error, bincode::Encode,  bincode::Decode)]
pub struct WALEntry {
    key: Key,
    entry: Entry,
}

impl std::fmt::Display for WALEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WAL Entry {{ key: {:?}, entry: \"{:?}\" }}", self.key, self.entry)
    }
}

impl WALEntry {
    pub fn deserialize(entry_bytes:  &Vec<u8>) -> Result<&Self, WALError> {
        let entry =  bincode::decode_from_slice(&entry_bytes, bincode::config::standard())?;

        Ok(entry);
    }
}

pub enum WALError {
    IoError(io::Error),
    SerializationEncodeError(EncodeError),
    SerializationDecodeError(DecodeError),
    InvalidEntrySize(u32),
}

impl From<io::Error> for WALError {
    fn from(err:  io::Error) -> Self {
        WALError::IoError(err)
    }
}

impl From<EncodeError> for WALError {
    fn from(err: EncodeError) -> Self {
        WALError::SerializationEncodeError(err)
    }
}

impl From<DecodeError> for WALError {
   fn from(err: DecodeError) -> Self {
        WALError::SerializationDecodeError(err)
    }
}

impl WAL {
    pub fn new(log_file_prefix:String, wal_folder: String) -> Self {
        WAL {
            wal_folder,
            log_file_prefix,
            sequential_counter: AtomicU64::new(0),
            current_file_id: 0,
            current_writer: None,
        }
    }
    fn ensure_writer(&mut self, file_id: u64) -> Result<(), WALError> {
        if self.current_writer.is_none() || self.current_file_id != file_id {
            if let Some(mut writer) = self.current_writer.take() {
                writer.flush()?;
            
            }
            let file = File::options()
                                    .create(true)
                                    .append(true)
                                    .open(format!("{}_{}.log", self.log_file_prefix, file_id))?;

            self.current_writer = Some(BufWriter::new(file));
            self.current_file_id = file_id;
        }

        Ok(())      
    }

    pub fn append(&mut self, key: Key, entry: Entry, current_file: u64) -> Result<(),WALError> {
        self.ensure_writer(current_file)?;
        let new_wal_entry = WALEntry{key: key, entry: entry};

        let b_entry = bincode::encode_to_vec(new_wal_entry, bincode::config::standard())?;

        let crc_dig = CRC_ALG.checksum(&b_entry);

        let writer = self.current_writer.as_mut().unwrap();  

        let entry_len = b_entry.len() as u32; 

        writer.write_all(&entry_len.to_le_bytes())?;
        writer.write_all(&b_entry)?;
        writer.write_all(&crc_dig.to_le_bytes())?;

        return Ok(())
    }

    pub fn discover_wal_files(&self) -> Result<Vec<u64>, WALError> {
        let dir = Path::new(&self.wal_folder);
        let mut file_ids = Vec::new();

        for entry in fs::read_dir(dir).unwrap() {
            let entry = entry?;
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();
            
            // Parse files matching pattern: "prefix_ID.log"
            if file_name_str.starts_with(&self.log_file_prefix) && file_name_str.ends_with(".log") {
                let middle = &file_name_str[self.log_file_prefix.len()..file_name_str.len()-4];
                if middle.starts_with('_') {
                    if let Ok(file_id) = middle[1..].parse::<u64>() {
                        file_ids.push(file_id);
                    }
                }
            }
        }

        Ok(file_ids)
    }

    pub fn recover_all(&self) -> Result<Vec<WALEntry>, WALError> {
        let file_ids = self.discover_wal_files()?;
        let mut all_entries = Vec::new();
        
        for file_id in file_ids {
            let mut entries = self.recover_from_file(file_id)?;
            all_entries.append(&mut entries);
        }
        
        Ok(all_entries)
    }

    pub fn get_next_file_id(&self) -> Result<u64, WALError> {
        let file_ids = self.discover_wal_files()?;
        Ok(file_ids.last().map(|&id| id + 1).unwrap_or(1))
    }

    pub fn recover_from_file(&self, file_id: u64) -> Result<Vec<WALEntry>, WALError> {
        let mut file = File::open(&format!("{}_{}.log", self.log_file_prefix, file_id))?;
        let mut buffer = Vec::<u8>::new();
        let mut entries = Vec::<WALEntry>::new();

        loop {
            let mut len_bytes = [0u8; 4];

            match file.read_exact(&mut len_bytes) {
                Ok(()) => {},
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(WALError::IoError(e)),
            }

            let entry_len = u32::from_le_bytes(len_bytes);

            // Validate entry length to prevent excessive memory allocation
            if entry_len > MAX_ENTRY_SIZE {
                return Err(WALError::InvalidEntrySize(entry_len));
            }

            buffer.resize(entry_len as usize, 0);
            file.read_exact(&buffer);

            match WALEntry::deserialize(&buffer) {
                Ok(entry) => entries.push(*entry),
                Err(e) => return Err(e),
            }
        }

        return Ok(entries)
    }
}