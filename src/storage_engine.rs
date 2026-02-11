use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{ErrorKind, Read, Write};
use std::iter::repeat_with;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;

use crate::fsutil;
use crate::transaction_log;
use anyhow::{Context, anyhow};
use camino::{Utf8Path, Utf8PathBuf};
use serde::{Deserialize, Serialize};

const MIN_DISKS: usize = 3;
const CHUNK_SIZE: usize = 1024 * 1024; // 1 MB
const NUM_LOCKS: usize = 1024;

struct LockPool {
    locks: Vec<Mutex<()>>,
}

impl LockPool {
    fn new() -> Self {
        Self {
            locks: Vec::from_iter(repeat_with(|| Mutex::new(())).take(NUM_LOCKS)),
        }
    }

    fn get_lock<K>(&self, key: &K) -> &Mutex<()>
    where
        K: Hash,
    {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        &self.locks[(hasher.finish() as usize) % self.locks.len()]
    }
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
    transaction_log: transaction_log::TransactionLog,
}

type TxnId = u64;

impl Disk {
    pub fn create(path: PathBuf, uuid: uuid::Uuid, pack: Vec<uuid::Uuid>) -> anyhow::Result<Self> {
        fs::create_dir_all(&path)?;
        fs::create_dir(path.join("wip"))?;
        fs::create_dir(path.join("backup"))?;
        fs::create_dir(path.join("objects"))?;

        let config_text = toml::to_string_pretty(&DiskConfig {
            uuid,
            pack: pack.clone(),
            generation: 0,
        })?;

        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path.join("config.toml"))
            .and_then(|mut file| file.write_all(config_text.as_bytes()))
            .with_context(|| "")?;

        let transaction_log = transaction_log::TransactionLog::new(path.join("wal"))?;

        Ok(Disk {
            path,
            uuid,
            pack,
            transaction_log,
        })
    }

    pub fn load(path: PathBuf) -> anyhow::Result<Self> {
        let config_text = std::fs::read_to_string(path.join("config.toml"))?;
        let config: DiskConfig = toml::from_str(&config_text)?;
        let transaction_log = transaction_log::TransactionLog::new(path.join("wal"))?;

        let disk = Disk {
            path,
            uuid: config.uuid,
            pack: config.pack,
            transaction_log,
        };

        Ok(disk)
    }

    pub fn get_wip_path(&self, txnid: TxnId) -> PathBuf {
        self.path.join("wip").join(txnid.to_string())
    }

    pub fn get_backup_path(&self, txnid: TxnId) -> PathBuf {
        self.path.join("backup").join(txnid.to_string())
    }

    pub fn get_object_path(&self, key: &Utf8Path) -> PathBuf {
        self.path.join("objects").join(key)
    }

    pub fn list_wip_txns(&self) -> anyhow::Result<Vec<TxnId>> {
        let mut txns: Vec<TxnId> = Vec::new();
        for entry in fs::read_dir(self.path.join("wip"))? {
            let entry = entry?;
            let filename = entry.file_name();
            let uuid = u64::from_str(filename.to_str().ok_or_else(|| {
                anyhow!(format!("Invalid UTF-8 in transaction ID: {:?}", filename))
            })?)
            .with_context(|| format!("Invalid transaction ID: {:?}", filename))?;
            txns.push(uuid);
        }

        Ok(txns)
    }
}

pub struct StorageEngine {
    disks: Vec<Disk>,
    txn_counter: AtomicU64,
    uncommitted: dashmap::DashMap<Utf8PathBuf, HashSet<TxnId>>,
    commit_locks: LockPool,
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
        for (path, uuid) in disk_paths.iter().zip(&uuids) {
            let disk = Disk::create(path.to_owned(), *uuid, uuids.to_owned())
                .with_context(|| format!("{:?}", path))?;
            disks.push(disk);
        }

        Ok(Self {
            disks,
            txn_counter: AtomicU64::new(0),
            uncommitted: dashmap::DashMap::new(),
            commit_locks: LockPool::new(),
        })
    }

    pub fn load(disk_paths: &[PathBuf]) -> anyhow::Result<Self> {
        let disks = disk_paths
            .iter()
            .map(|path| Disk::load(path.clone()))
            .collect::<anyhow::Result<Vec<_>>>()?;

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
            txn_counter: AtomicU64::new(0),
            uncommitted: dashmap::DashMap::new(),
            commit_locks: LockPool::new(),
        };
        engine.cleanup_incomplete()?;
        Ok(engine)
    }

    fn cleanup_incomplete(&self) -> anyhow::Result<()> {
        Ok(())
    }

    pub fn get(&self, key: &Utf8Path, sink: &mut impl Write) -> anyhow::Result<()> {
        let txns = self.uncommitted.get(key);
        if self.uncommitted.contains_key(key) {
            return Err(anyhow!("Key is currently being written"));
        }

        let obj_path = self.disks[0].get_object_path(key);
        let mut obj_file = OpenOptions::new().read(true).open(obj_path)?;
        std::io::copy(&mut obj_file, sink)?;
        sink.flush()?;

        Ok(())
    }

    pub fn put(&self, key: &Utf8Path, reader: &mut impl Read) -> anyhow::Result<()> {
        let txnid = self
            .txn_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let key_str = key.as_str();
        for disk in &self.disks {
            disk.transaction_log.append(
                transaction_log::WalType::Write,
                txnid,
                key_str,
                transaction_log::WalState::Prepared,
            )?;
        }

        self.uncommitted
            .entry(key.to_owned())
            .or_default()
            .insert(txnid);

        // Write to each WIP directory
        let mut filenames: Vec<(PathBuf, PathBuf)> = Vec::with_capacity(self.disks.len());
        {
            let mut buf = vec![0u8; CHUNK_SIZE];
            let mut disk_files = Vec::with_capacity(self.disks.len());
            for disk in &self.disks {
                let wip_path = disk.get_wip_path(txnid);
                filenames.push((wip_path.clone(), disk.get_object_path(key)));
                let wip_file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(wip_path)?;
                disk_files.push(wip_file);
            }

            loop {
                let filled = fsutil::read_retry_on_intr(reader, &mut buf)?;
                if filled.is_empty() {
                    break;
                }

                for wip_file in &mut disk_files {
                    wip_file.write_all(filled)?;
                }
            }

            for wip_file in &disk_files {
                wip_file.sync_data()?;
            }
        }

        // Start the commit
        let commit_lock = self.commit_locks.get_lock(&key).lock().unwrap();

        // Create parent dirs
        for (_, final_path) in &filenames {
            fs::create_dir_all(final_path.parent().unwrap())?;
        }

        for disk in &self.disks {
            let obj_path = disk.get_object_path(key);
            let backup_path = disk.get_backup_path(txnid);

            // Move old version to backup (if exists)
            if obj_path.exists() {
                match fs::rename(&obj_path, &backup_path) {
                    Ok(_) => {}
                    Err(e) if e.kind() == ErrorKind::NotFound => {}
                    Err(e) => return Err(e).with_context(|| "Error creating commit backup links"),
                }
            }

            // Rename wip to objects
            fs::create_dir_all(obj_path.parent().unwrap())?;
        }

        // Move each to its final location
        for (wip_path, final_path) in &filenames {
            std::fs::rename(wip_path, final_path)?;
        }

        // Mark transaction complete
        for disk in &self.disks {
            disk.transaction_log.append(
                transaction_log::WalType::Write,
                txnid,
                key_str,
                transaction_log::WalState::Committed,
            )?;
        }

        self.uncommitted.remove_if_mut(key, |_k, set| {
            set.remove(&txnid);
            set.is_empty()
        });

        drop(commit_lock);

        // Remove backup files
        for disk in &self.disks {
            std::fs::remove_file(disk.get_backup_path(txnid))?;
        }

        Ok(())
    }
}
