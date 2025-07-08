use bincode::enc::Encode;
use bincode::de::Decode;
use bincode::error::{EncodeError, DecodeError};
use std::io::{self, BufWriter, Take, Write};
use std::{fs::File};
use std::sync::atomic::AtomicU64;
use crate::lsm_tree::memtable::{Entry, Key};

const CRC_ALG: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_CKSUM);

pub struct WAL {
    wal_log_folder: String,
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

pub enum WALError {
    IoError(io::Error),
    SerializationEncodeError(EncodeError),
    SerializationDecodeError(DecodeError),
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
    pub fn new(log_file_prefix:String) -> Self {
        WAL {
            log_file_prefix: log_file_prefix,
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
}