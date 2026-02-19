use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;

use parking_lot::{Mutex, RwLock};

use crate::fsutil;
use crate::transaction_log::{self, TransactionLog};
use anyhow::{Context, anyhow};
use camino::{Utf8Path, Utf8PathBuf};
use serde::{Deserialize, Serialize};
use xattr::FileExt;

const MIN_DISKS: usize = 3;
const MARKER_FILENAME: &str = "yote.marker";
const DELETE_SUFFIX: &str = ".deleted";
const WAL_FILENAME: &str = "wal";
const CONFIG_FILENAME: &str = "config.toml";

const PERCENT_ENCODE_SET: percent_encoding::AsciiSet = percent_encoding::AsciiSet::EMPTY.add(b'.');

fn clean_key(key: &Utf8Path) -> Utf8PathBuf {
    key.components()
        .map(|component| {
            percent_encoding::utf8_percent_encode(component.as_str(), &PERCENT_ENCODE_SET)
                .to_string()
        })
        .collect()
}

fn unclean_key(key: &Utf8Path) -> anyhow::Result<Utf8PathBuf> {
    let mut out = Utf8PathBuf::new();

    for component in key.components() {
        let decoded = percent_encoding::percent_decode_str(component.as_str()).decode_utf8()?;
        out.push(decoded.as_ref());
    }

    Ok(out)
}

enum KeyMatch {
    Object(PathBuf),
    Deleted,
}

#[derive(Deserialize, Serialize)]
struct DiskConfig {
    uuid: uuid::Uuid,
    pack: Vec<uuid::Uuid>,
    generation: u32,
}

struct Disk {
    path: PathBuf,
    uuid: uuid::Uuid,
    pack: Vec<uuid::Uuid>,
}

type TxnId = u64;

impl Disk {
    pub fn create(
        path: PathBuf,
        uuid: uuid::Uuid,
        pack: Vec<uuid::Uuid>,
    ) -> anyhow::Result<(Self, TransactionLog)> {
        fs::create_dir_all(&path)?;
        fs::create_dir(path.join("wip"))?;
        fs::create_dir(path.join("objects"))?;

        let config_text = toml::to_string_pretty(&DiskConfig {
            uuid,
            pack: pack.clone(),
            generation: 0,
        })?;

        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path.join(CONFIG_FILENAME))
            .and_then(|mut file| file.write_all(config_text.as_bytes()))
            .with_context(|| "")?;

        let transaction_log = transaction_log::TransactionLog::new(path.join(WAL_FILENAME))?;

        Ok((Disk { path, uuid, pack }, transaction_log))
    }

    pub fn load(path: PathBuf) -> anyhow::Result<(Self, TransactionLog)> {
        let config_text = std::fs::read_to_string(path.join(CONFIG_FILENAME))?;
        let config: DiskConfig = toml::from_str(&config_text)?;
        let transaction_log = transaction_log::TransactionLog::new(path.join(WAL_FILENAME))?;

        let disk = Disk {
            path,
            uuid: config.uuid,
            pack: config.pack,
        };

        Ok((disk, transaction_log))
    }

    pub fn get_wip_root(&self) -> PathBuf {
        self.path.join("wip")
    }

    pub fn get_wip_path(&self, txnid: TxnId) -> PathBuf {
        self.get_wip_root().join(txnid.to_string())
    }

    pub fn get_object_root(&self) -> PathBuf {
        self.path.join("objects")
    }

    pub fn get_object_path(&self, key: &Utf8Path) -> PathBuf {
        self.get_object_root().join(key)
    }

    pub fn choose_highest_txn(
        &self,
        key: &Utf8Path,
        predicate: impl Fn(TxnId) -> bool,
    ) -> anyhow::Result<Option<KeyMatch>> {
        let _versions: Vec<TxnId> = Vec::new();
        let mut choice = None;

        let prefix_path = self.get_object_path(key);

        for entry in fs::read_dir(prefix_path)? {
            let entry = entry?;
            let raw_filename = entry.file_name();
            let filename = raw_filename.to_str().ok_or_else(|| {
                anyhow!(format!(
                    "Invalid UTF-8 in transaction ID: {:?}",
                    entry.file_name()
                ))
            })?;

            let stripped_tombstone = filename.trim_end_matches(DELETE_SUFFIX);

            let (txnid, is_tombstone): (&str, bool) = if filename.len() > stripped_tombstone.len() {
                (stripped_tombstone, true)
            } else {
                (filename, false)
            };

            let txnid = match u64::from_str(txnid) {
                Ok(txnid) => txnid,
                Err(_) => continue,
            };

            if !predicate(txnid) {
                continue;
            }

            let get_keymatch = || {
                if is_tombstone {
                    KeyMatch::Deleted
                } else {
                    KeyMatch::Object(entry.path().to_owned())
                }
            };

            match choice {
                None => choice = Some((txnid, get_keymatch())),
                Some((old_txnid, _)) => {
                    if txnid > old_txnid {
                        choice = Some((txnid, get_keymatch()))
                    }
                }
            }
        }

        Ok(choice.map(|(_, path)| path))
    }
}

pub struct StorageEngine {
    disks: Vec<Disk>,
    wals: RwLock<Vec<TransactionLog>>,
    txn_alloc_mutex: Mutex<()>,
    txn_counter: AtomicU64,
    uncommitted: dashmap::DashSet<TxnId>,
}

impl StorageEngine {
    pub fn create(disk_paths: &[PathBuf], shards: (usize, usize)) -> anyhow::Result<Self> {
        if disk_paths.len() < MIN_DISKS {
            return Err(anyhow!("At least 3 disks must be provisioned"));
        }

        if shards.0 < 1 || shards.1 < 1 {
            return Err(anyhow!("At least 1 data shard and 1 parity shard required"));
        }

        if disk_paths.len() < (shards.0 + shards.1) {
            return Err(anyhow!("Not enough disks for shard configuration"));
        }

        if shards.0 <= shards.1 {
            return Err(anyhow!("Need more data shards than parity shards"));
        }

        let mut disks: Vec<Disk> = Vec::with_capacity(disk_paths.len());
        let mut uuids: Vec<_> = (0..disk_paths.len())
            .map(|_| uuid::Uuid::new_v4())
            .collect();
        uuids.sort();
        let mut wals = Vec::with_capacity(disk_paths.len());
        for (path, uuid) in disk_paths.iter().zip(&uuids) {
            let (disk, wal) = Disk::create(path.to_owned(), *uuid, uuids.to_owned())
                .with_context(|| format!("{:?}", path))?;
            disks.push(disk);
            wals.push(wal);
        }

        Ok(Self {
            disks,
            wals: RwLock::new(wals),
            txn_alloc_mutex: Mutex::new(()),
            txn_counter: AtomicU64::new(0),
            uncommitted: dashmap::DashSet::new(),
        })
    }

    pub fn load(disk_paths: &[PathBuf]) -> anyhow::Result<Self> {
        let (disks, wals): (Vec<Disk>, Vec<TransactionLog>) = disk_paths
            .iter()
            .map(|path| Disk::load(path.clone()))
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter()
            .unzip();

        // Ensure all disks know they're all in the same pack
        let mut pack_uuids: Vec<uuid::Uuid> = disks.iter().map(|d| d.uuid).collect();
        pack_uuids.sort();
        for disk in &disks {
            if disk.pack != pack_uuids {
                return Err(anyhow!("Disks are not in the same pack"));
            }
        }

        let engine = StorageEngine {
            disks,
            wals: RwLock::new(wals),
            txn_alloc_mutex: Mutex::new(()),
            txn_counter: AtomicU64::new(0),
            uncommitted: dashmap::DashSet::new(),
        };
        engine.cleanup_incomplete()?;
        Ok(engine)
    }

    fn cleanup_incomplete(&self) -> anyhow::Result<()> {
        struct TransactionRecord {
            key: Utf8PathBuf,
            committed_set: std::collections::HashSet<uuid::Uuid>,
        }

        let mut txns: std::collections::HashMap<TxnId, TransactionRecord> =
            std::collections::HashMap::new();
        // Reconcile WALs
        {
            let mut wals = self.wals.write();
            for (disk, wal) in self.disks.iter().zip(wals.iter_mut()) {
                for record in wal.iterate()? {
                    let record = record?;
                    let txn_entry = txns.entry(record.txid).or_insert(TransactionRecord {
                        key: Utf8PathBuf::from(&record.key),
                        committed_set: std::collections::HashSet::new(),
                    });
                    assert_eq!(
                        txn_entry.key,
                        Utf8Path::new(&record.key),
                        "txnid {} has inconsistent keys across disks: expected {}, got {}",
                        record.txid,
                        txn_entry.key,
                        record.key
                    );

                    match record.state {
                        transaction_log::WalState::Prepared => {}
                        transaction_log::WalState::Committed => {
                            txn_entry.committed_set.insert(disk.uuid);
                        }
                        transaction_log::WalState::Aborted => {}
                    }
                }
            }
        }

        // Clean up transactions we've now established are incomplete
        let disk_uuids: std::collections::HashSet<uuid::Uuid> =
            self.disks.iter().map(|disk| disk.uuid).collect();
        for (txnid, txn) in txns.iter() {
            if txn.committed_set != disk_uuids {
                // Remove all artifacts associated with the aborted txnid
                if let Err(cleanup_err) = self.cleanup(&txn.key, *txnid) {
                    panic!(
                        "Failed to cleanup transaction at startup {}: {}",
                        txnid, cleanup_err
                    );
                }
            }
        }

        // Choose a next transaction ID
        // Really we don't have to lock this mutex since this must only be called at startup,
        // but it's good hygiene
        let _guard = self.txn_alloc_mutex.lock();
        self.txn_counter.store(
            txns.keys().max().map(|txnid| txnid + 1).unwrap_or(0),
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(())
    }

    pub fn get(&self, key: &Utf8Path, sink: &mut impl Write) -> anyhow::Result<Option<()>> {
        let key = clean_key(key);

        let max_txnid = self.txn_counter.load(std::sync::atomic::Ordering::Acquire);
        let obj_path = match self.disks[0].choose_highest_txn(&key, |txnid| {
            txnid < max_txnid && !self.uncommitted.contains(&txnid)
        })? {
            Some(KeyMatch::Object(path)) => path,
            _ => return Ok(None),
        };

        let mut obj_file = OpenOptions::new().read(true).open(obj_path)?;
        std::io::copy(&mut obj_file, sink)?;
        sink.flush()?;

        Ok(Some(()))
    }

    pub fn list(&self, prefix: &Utf8Path) -> anyhow::Result<Vec<Utf8PathBuf>> {
        let prefix = clean_key(prefix);

        let uncommitted_txnids_snapshot = self.uncommitted.clone();
        let max_txnid = self.txn_counter.load(std::sync::atomic::Ordering::Acquire);
        let disk = &self.disks[0];

        let mut result: Vec<Utf8PathBuf> = Vec::new();

        for entry in walkdir::WalkDir::new(disk.get_object_path(&prefix)).sort_by_file_name() {
            let entry = entry?;

            if !entry.file_type().is_file() {
                continue;
            }

            let path = entry.path();
            if path.file_name() != Some(std::ffi::OsStr::new(MARKER_FILENAME)) {
                continue;
            }

            let key = Utf8Path::from_path(path.parent().unwrap())
                .ok_or_else(|| anyhow::anyhow!("Non-UTF8 path: {}", path.display()))?;

            let relative_key = key
                .strip_prefix(disk.get_object_root())
                .context("Failed to strip object root prefix")?;

            if let Some(KeyMatch::Object(_)) = disk.choose_highest_txn(relative_key, |txnid| {
                txnid < max_txnid && !uncommitted_txnids_snapshot.contains(&txnid)
            })? {
                result.push(unclean_key(relative_key).with_context(|| {
                    format!(
                        "Invalid non-UTF8 path in escape characters: {}",
                        relative_key.as_str()
                    )
                })?);
            }
        }

        Ok(result)
    }

    fn _put(
        &self,
        key: &Utf8Path,
        mut reader: Option<&mut dyn Read>,
        txnid: TxnId,
        metadata: &[(&str, &str)],
    ) -> anyhow::Result<()> {
        self.uncommitted.insert(txnid);

        // Write our prepare message to WALs
        let mut wals = self.wals.write();
        for (_disk, wal) in self.disks.iter().zip(wals.iter_mut()) {
            wal.append(txnid, key.as_str(), transaction_log::WalState::Prepared)?;
        }
        drop(wals);
        for wal in self.wals.read().iter() {
            wal.fsync()?;
        }

        // Create WIP files
        let wip_paths: Vec<_> = self
            .disks
            .iter()
            .map(|disk| disk.get_wip_path(txnid))
            .collect();
        let mut wip_files: Vec<_> = wip_paths
            .iter()
            .map(|path| {
                std::fs::File::create(path)
                    .with_context(|| format!("Failed to create file at {}", path.display()))
            })
            .collect::<anyhow::Result<_>>()?;
        for wip_file in wip_files.iter_mut() {
            for (key, value) in metadata {
                wip_file.set_xattr(format!("user.{}", key), value.as_bytes())?;
            }
        }
        if let Some(reader) = &mut reader {
            let mut multi_writer = fsutil::MultiWriter::new(wip_files.iter_mut().collect());
            std::io::copy(reader, &mut multi_writer)?;
            for wip_file in wip_files.iter() {
                wip_file.sync_data()?;
            }
        }
        fsutil::sync_paths(self.disks.iter().map(|disk| disk.get_wip_root()))?;

        // Rename WIP to final locations
        let object_paths: Vec<_> = self
            .disks
            .iter()
            .map(|disk| disk.get_object_path(key))
            .collect();
        for object_path in &object_paths {
            std::fs::create_dir_all(object_path)?;
        }
        for object_path in &object_paths {
            std::fs::File::create(object_path.join(MARKER_FILENAME))?;
        }
        let filename = if reader.is_some() {
            txnid.to_string()
        } else {
            format!("{}{}", txnid, DELETE_SUFFIX)
        };
        let final_paths: Vec<_> = object_paths
            .iter()
            .map(|path| path.join(&filename))
            .collect();
        for (wip_path, final_path) in wip_paths.iter().zip(final_paths.iter()) {
            std::fs::rename(wip_path, final_path)?;
        }
        fsutil::sync_paths(&object_paths)?;

        // Write committed to the WAL
        let mut wals = self.wals.write();
        for wal in wals.iter_mut() {
            wal.append(txnid, key.as_str(), transaction_log::WalState::Committed)?;
        }
        drop(wals);
        for wal in self.wals.read().iter() {
            wal.fsync()?;
        }

        Ok(())
    }

    pub fn put(
        &self,
        key: &Utf8Path,
        reader: Option<&mut dyn Read>,
        metadata: &[(&str, &str)],
    ) -> anyhow::Result<()> {
        let key = clean_key(key);

        let txnid = {
            let _guard = self.txn_alloc_mutex.lock();
            let txnid = self
                .txn_counter
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            self.uncommitted.insert(txnid);
            txnid
        };

        let result = self._put(&key, reader, txnid, metadata);
        if result.is_err()
            && let Err(cleanup_error) = self.cleanup(&key, txnid)
        {
            panic!("Failed to cleanup transaction {}: {}", txnid, cleanup_error);
        }
        self.uncommitted.remove(&txnid);
        result
    }

    fn cleanup(&self, key: &Utf8Path, txnid: u64) -> anyhow::Result<()> {
        let object_paths: Vec<_> = self
            .disks
            .iter()
            .map(|disk| disk.get_object_path(key))
            .collect();

        for path in object_paths.iter().map(|disk| disk.join(txnid.to_string())) {
            fsutil::ignore_errorkind(std::fs::remove_file(path), std::io::ErrorKind::NotFound)?;
        }

        for wip_path in self.disks.iter().map(|disk| disk.get_wip_path(txnid)) {
            fsutil::ignore_errorkind(std::fs::remove_file(wip_path), std::io::ErrorKind::NotFound)?;
        }
        fsutil::ignore_errorkind(
            fsutil::sync_paths(&object_paths),
            std::io::ErrorKind::NotFound,
        )?;
        fsutil::sync_paths(self.disks.iter().map(|disk| disk.get_wip_root()))?;

        // Write aborted to the WAL
        let mut wals = self.wals.write();
        for wal in wals.iter_mut() {
            wal.append(txnid, key.as_str(), transaction_log::WalState::Aborted)?;
        }
        drop(wals);
        for wal in self.wals.read().iter() {
            wal.fsync()?;
        }

        Ok(())
    }
}
