use std::io::{Read, Seek, Write};
use std::path::PathBuf;

use anyhow::Context;
use capnp::message;
use xxhash_rust::xxh3::Xxh3;

use crate::fsutil;
use crate::txnlog_capnp;

pub use txnlog_capnp::State as WalState;
pub use txnlog_capnp::Type as WalType;

fn read_exact_or_eof<R: Read, const N: usize>(reader: &mut R) -> anyhow::Result<Option<[u8; N]>> {
    let mut buf = [0u8; N];

    let first_byte_read = fsutil::read_retry_on_intr(reader, &mut buf[..1])?.len();

    match first_byte_read {
        0 => Ok(None),
        1 => {
            reader
                .read_exact(&mut buf[1..])
                .context("incomplete read after initial byte")?;
            Ok(Some(buf))
        }
        _ => unreachable!("read into 1-byte buffer returned > 1"),
    }
}

pub fn write_segment_trailer<W>(mut writer: W, buf: &[u8]) -> anyhow::Result<()>
where
    W: Write,
{
    let mut hasher = Xxh3::new();
    let len_bytes = (buf.len() as u32).to_le_bytes();
    hasher.update(&len_bytes);
    hasher.update(buf);
    let digest_bytes = hasher.digest128().to_le_bytes();

    writer.write_all(&len_bytes)?;
    writer.write_all(&digest_bytes)?;
    writer.write_all(buf)?;

    Ok(())
}

pub fn read_segment_trailer<R>(mut reader: R, buf: &mut Vec<u8>) -> anyhow::Result<Option<()>>
where
    R: Read,
{
    // Read length
    let len_bytes: [u8; 4] = match read_exact_or_eof(&mut reader)? {
        Some(buf) => buf,
        None => {
            buf.clear();
            return Ok(None);
        }
    };
    let len = u32::from_le_bytes(len_bytes) as usize;

    // Read expected digest
    let mut expected_digest = [0u8; 16];
    reader.read_exact(&mut expected_digest)?;

    // Read payload
    buf.clear();
    buf.resize(len, 0);

    reader.read_exact(buf)?;

    // Recompute hash
    let mut hasher = Xxh3::new();
    hasher.update(&len_bytes);
    hasher.update(buf);
    let actual_digest = hasher.digest128().to_le_bytes();

    // Verify
    anyhow::ensure!(
        actual_digest == expected_digest,
        "segment trailer checksum mismatch"
    );

    Ok(Some(()))
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub entry_type: WalType,
    pub txid: u64,
    pub key: String,
    pub state: WalState,
}

pub struct TransactionLogIterator<'a, R: Read> {
    buf: Vec<u8>,
    errored: bool,
    reader: std::sync::MutexGuard<'a, R>,
}

impl<'a, R: Read> Iterator for TransactionLogIterator<'a, R> {
    type Item = anyhow::Result<LogEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.errored {
            return None;
        }

        let result = (|| -> anyhow::Result<Option<LogEntry>> {
            if read_segment_trailer(&mut *self.reader, &mut self.buf)?.is_none() {
                return Ok(None);
            }

            let message_reader = capnp::serialize::read_message(
                std::io::Cursor::new(&self.buf),
                message::ReaderOptions::new(),
            )?;

            let entry = message_reader.get_root::<txnlog_capnp::transaction_log_entry::Reader>()?;

            Ok(Some(LogEntry {
                entry_type: entry.get_type()?,
                txid: entry.get_txid(),
                key: entry.get_key()?.to_string()?,
                state: entry.get_state()?,
            }))
        })();

        if result.is_err() {
            self.errored = true;
        }

        result.transpose()
    }
}

#[derive(Debug)]
pub struct TransactionLog {
    path: PathBuf,
    file: std::sync::Mutex<std::fs::File>,
}

impl TransactionLog {
    pub fn new(path: PathBuf) -> anyhow::Result<Self> {
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        file.try_lock()
            .with_context(|| "Transaction log already locked")?;
        Ok(Self {
            path,
            file: std::sync::Mutex::new(file),
        })
    }

    pub fn append(&self, ty: WalType, txid: u64, key: &str, state: WalState) -> anyhow::Result<()> {
        let mut message = ::capnp::message::Builder::new_default();

        let mut log_entry = message.init_root::<txnlog_capnp::transaction_log_entry::Builder>();
        log_entry.set_type(ty);
        log_entry.set_txid(txid);
        log_entry.set_state(state);
        log_entry.set_key(key);

        let mut message_buf = Vec::new();
        capnp::serialize::write_message(&mut message_buf, &message)?;

        let mut file_guard = self.file.lock().unwrap();
        (*file_guard).seek(std::io::SeekFrom::End(0))?;

        {
            let writer = std::io::BufWriter::new(&*file_guard);
            write_segment_trailer(writer, &message_buf)?;
        }

        file_guard.sync_all()?;

        Ok(())
    }

    pub fn iterate(&self) -> anyhow::Result<TransactionLogIterator<'_, std::fs::File>> {
        let mut file_guard = self.file.lock().unwrap();
        (*file_guard).rewind()?;
        Ok(TransactionLogIterator {
            buf: Vec::new(),
            errored: false,
            reader: file_guard,
        })
    }

    pub fn clear(&self) -> anyhow::Result<()> {
        let mut file_guard = self.file.lock().unwrap();
        file_guard.rewind()?;
        file_guard.set_len(0)?;
        file_guard.sync_all()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_new_with_existing_file() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        // Create file first
        std::fs::write(&log_path, b"existing content").unwrap();

        TransactionLog::new(log_path.clone()).unwrap();
        assert!(log_path.exists());
    }

    #[test]
    fn test_lock_prevents_second_instance() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        let _log1 = TransactionLog::new(log_path.clone()).unwrap();
        let log2 = TransactionLog::new(log_path.clone());

        assert!(log2.is_err());
    }

    #[test]
    fn test_iterate_empty_log() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        let log = TransactionLog::new(log_path).unwrap();

        let mut iter = log.iterate().unwrap();
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_append_and_iterate_multiple_entries() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        let log = TransactionLog::new(log_path).unwrap();

        let entries = vec![
            (WalType::Write, "key1", WalState::Prepared),
            (WalType::Write, "key2", WalState::Committed),
            (WalType::Delete, "key3", WalState::Aborted),
        ];

        for (ty, key, state) in &entries {
            log.append(*ty, 0, key, *state).unwrap();
        }

        let mut iter = log.iterate().unwrap();
        for (expected_ty, expected_key, expected_state) in entries {
            let entry = iter.next().unwrap().unwrap();
            assert_eq!(entry.entry_type, expected_ty);
            assert_eq!(entry.key, expected_key);
            assert_eq!(entry.state, expected_state);
        }

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_clear_empty_log() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        let log = TransactionLog::new(log_path.clone()).unwrap();

        log.clear().unwrap();

        let metadata = std::fs::metadata(&log_path).unwrap();
        assert_eq!(metadata.len(), 0);
    }

    #[test]
    fn test_clear_with_entries() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        let log = TransactionLog::new(log_path.clone()).unwrap();

        log.append(WalType::Write, 0, "key1", WalState::Prepared)
            .unwrap();

        log.append(WalType::Write, 1, "key2", WalState::Committed)
            .unwrap();

        log.clear().unwrap();

        let metadata = std::fs::metadata(&log_path).unwrap();
        assert_eq!(metadata.len(), 0);

        // Verify iteration returns nothing
        let mut iter = log.iterate().unwrap();
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_append_after_clear() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        let log = TransactionLog::new(log_path.clone()).unwrap();

        log.append(WalType::Write, 0, "old_key", WalState::Prepared)
            .unwrap();

        log.clear().unwrap();

        log.append(WalType::Write, 1, "new_key", WalState::Committed)
            .unwrap();

        let iter = log.iterate().unwrap();
        let entries: Vec<String> = iter.map(|e| e.unwrap().key).collect();
        assert_eq!(entries, vec!["new_key"]);
    }

    #[test]
    fn test_append_with_special_characters() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        let log = TransactionLog::new(log_path).unwrap();

        let special_keys = vec![
            "key with spaces",
            "key/with/slashes",
            "key:with:colons",
            "key_with_unicode_🔥",
            "", // empty string
        ];

        for key in &special_keys {
            log.append(WalType::Write, 0, key, WalState::Prepared)
                .unwrap();
        }

        let mut iter = log.iterate().unwrap();
        for expected_key in special_keys {
            let entry = iter.next().unwrap().unwrap();
            assert_eq!(entry.key, expected_key);
        }
    }

    #[test]
    fn test_concurrent_appends() {
        use std::sync::Arc;
        use std::thread;

        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        let log = Arc::new(TransactionLog::new(log_path).unwrap());

        let mut handles = vec![];
        for i in 0..10 {
            let log_clone = Arc::clone(&log);
            let handle = thread::spawn(move || {
                log_clone
                    .append(WalType::Write, 0, &format!("key_{}", i), WalState::Prepared)
                    .unwrap();
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Should have 10 entries (order may vary)
        let log_mut = Arc::try_unwrap(log).unwrap();
        let iter = log_mut.iterate().unwrap();
        assert_eq!(iter.count(), 10);
    }

    #[test]
    fn test_detect_corruption() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        let log = TransactionLog::new(log_path.clone()).unwrap();

        let keys = vec!["key 1", "key 2", "key 3"];

        for key in &keys {
            log.append(WalType::Write, 0, key, WalState::Prepared)
                .unwrap();
        }

        // Now let's corrupt the file ehehe
        {
            let mut naughty_file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&log_path)
                .unwrap();
            let mut contents = Vec::new();
            naughty_file.read_to_end(&mut contents).unwrap();
            let substring = "key 2".as_bytes();
            if let Some(pos) = contents
                .windows(substring.len())
                .position(|window| window == substring)
            {
                naughty_file
                    .seek(std::io::SeekFrom::Start(pos as u64))
                    .unwrap();
                naughty_file.write_all(&[0xFF]).unwrap(); // Corrupt the first byte of the substring
            }
        }

        assert_eq!(
            log.iterate()
                .unwrap()
                .map(|entry| entry.map(|e| e.key).map_err(|e| e.to_string()))
                .collect::<Vec<Result<String, String>>>(),
            vec![
                Ok("key 1".to_string()),
                Err("segment trailer checksum mismatch".to_string())
            ]
        );
    }
}
