use std::{path::{Path, PathBuf}, sync::{Arc, RwLock, atomic::{AtomicBool, Ordering}}, time::{SystemTime, UNIX_EPOCH}};

use anyhow::{Context, Result};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use sha2::Digest;
use crate::{binary, model::Operation};

const DEDUP: TableDefinition<&[u8], &[u8]> = TableDefinition::new("dedup_v1");
const META: TableDefinition<u8, u64> = TableDefinition::new("dedup_meta_v1");
const HISTORY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("operation_history_v1");
const BY_OWNER: TableDefinition<&[u8], &[u8]> = TableDefinition::new("operation_by_owner_v1");
const BY_SKU: TableDefinition<&[u8], &[u8]> = TableDefinition::new("operation_by_sku_v1");
const BLOOM_BYTES: usize = 32 * 1024 * 1024;
const HISTORY_SEQUENCE: u8 = 1;
const HISTORY_EPOCH: u8 = 2;
const HISTORY_BINARY_MIGRATION: u8 = 3;

#[derive(Clone)]
pub struct DiskDedup {
    database: Arc<Database>,
    history_database: Arc<Database>,
    bloom: Arc<RwLock<Vec<u8>>>,
    bloom_ready: Arc<AtomicBool>,
}

impl DiskDedup {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
        let existing_dedup = path.exists();
        let database = Database::create(path).context("cannot open disk dedup index")?;
        let history_path = path.with_file_name("history.redb");
        let history_database = Database::create(&history_path).context("cannot open disk history index")?;
        {
            let transaction = database.begin_write()?;
            {
                let _ = transaction.open_table(DEDUP)?;
                let _ = transaction.open_table(META)?;
            }
            transaction.commit()?;
        }
        {
            let transaction = history_database.begin_write()?;
            { let _ = transaction.open_table(META)?; let _ = transaction.open_table(HISTORY)?; let _ = transaction.open_table(BY_OWNER)?; let _ = transaction.open_table(BY_SKU)?; }
            transaction.commit()?;
        }
        let instance = Self {
            database: Arc::new(database),
            history_database: Arc::new(history_database),
            bloom: Arc::new(RwLock::new(vec![0; BLOOM_BYTES])),
            bloom_ready: Arc::new(AtomicBool::new(!existing_dedup)),
        };
        if existing_dedup { instance.rebuild_bloom_in_background(); }
        Ok(instance)
    }

    pub fn insert_operations(&self, entries: &[([u8; 32], [u8; 32], Operation)]) -> Result<()> {
        if entries.is_empty() { return Ok(()); }
        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::None)?;
        {
            let mut dedup = transaction.open_table(DEDUP)?;
            for (key, fingerprint, _) in entries {
                dedup.insert(key.as_slice(), fingerprint.as_slice())?;
            }
        }
        transaction.commit()?;
        let mut history_transaction = self.history_database.begin_write()?;
        history_transaction.set_durability(Durability::None)?;
        {
            let mut history = history_transaction.open_table(HISTORY)?;
            let mut by_owner = history_transaction.open_table(BY_OWNER)?;
            let mut by_sku = history_transaction.open_table(BY_SKU)?;
            for (key, _, operation) in entries {
                let payload = binary::encode_operation(operation)?;
                history.insert(key.as_slice(), payload.as_slice())?;
                let owner_key = index_key(&operation.owner_id, &operation.operation_id);
                let sku_key = index_key(&operation.sku, &operation.operation_id);
                by_owner.insert(owner_key.as_slice(), key.as_slice())?;
                by_sku.insert(sku_key.as_slice(), key.as_slice())?;
            }
        }
        history_transaction.commit()?;
        let mut bloom = self.bloom.write().expect("bloom lock");
        for (key, _, _) in entries { bloom_add(&mut bloom, key); }
        Ok(())
    }

    pub fn by_owner(&self, owner: &str, cursor: Option<&str>, limit: usize) -> Result<Vec<Operation>> {
        self.by_index(BY_OWNER, owner, cursor, limit)
    }

    pub fn by_sku(&self, sku: &str, cursor: Option<&str>, limit: usize) -> Result<Vec<Operation>> {
        self.by_index(BY_SKU, sku, cursor, limit)
    }

    fn by_index(&self, definition: TableDefinition<&[u8], &[u8]>, value: &str, cursor: Option<&str>, limit: usize) -> Result<Vec<Operation>> {
        let transaction = self.history_database.begin_read()?;
        let index = transaction.open_table(definition)?;
        let history = transaction.open_table(HISTORY)?;
        let prefix = index_prefix(value);
        let mut end = prefix.clone();
        end.push(0xff);
        let mut result = Vec::with_capacity(limit);
        for entry in index.range(prefix.as_slice()..end.as_slice())? {
            let (index_key, operation_key) = entry?;
            let id = std::str::from_utf8(&index_key.value()[prefix.len()..])?;
            if cursor.is_some_and(|cursor| id <= cursor) { continue; }
            if let Some(payload) = history.get(operation_key.value())? {
                result.push(binary::decode_operation(payload.value()).or_else(|_| serde_json::from_slice(payload.value()).context("invalid legacy JSON history record"))?);
                if result.len() == limit { break; }
            }
        }
        Ok(result)
    }

    pub fn get_operation(&self, key: &[u8; 32]) -> Result<Option<Operation>> {
        let transaction = self.history_database.begin_read()?;
        let table = transaction.open_table(HISTORY)?;
        let Some(value) = table.get(key.as_slice())? else { return Ok(None); };
        Ok(Some(binary::decode_operation(value.value()).or_else(|_| serde_json::from_slice(value.value()).context("invalid legacy JSON history record"))?))
    }

    pub fn get_operations(&self, keys: &[[u8; 32]]) -> Result<Vec<Operation>> {
        let transaction = self.history_database.begin_read()?;
        let table = transaction.open_table(HISTORY)?;
        let mut result = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(value) = table.get(key.as_slice())? {
                result.push(binary::decode_operation(value.value()).or_else(|_| serde_json::from_slice(value.value()).context("invalid legacy JSON history record"))?);
            }
        }
        Ok(result)
    }

    pub fn history_len(&self) -> Result<u64> {
        let transaction = self.history_database.begin_read()?;
        Ok(transaction.open_table(HISTORY)?.len()?)
    }

    pub fn history_progress(&self) -> Result<(u64, u64)> {
        let transaction = self.history_database.begin_read()?;
        let table = transaction.open_table(META)?;
        Ok((
            table.get(&HISTORY_SEQUENCE)?.map_or(0, |value| value.value()),
            table.get(&HISTORY_EPOCH)?.map_or(0, |value| value.value()),
        ))
    }

    pub fn prepare_binary_history_migration(&self) -> Result<(u64, u64)> {
        let read = self.history_database.begin_read()?;
        let state = read.open_table(META)?.get(&HISTORY_BINARY_MIGRATION)?.map_or(0, |value| value.value());
        drop(read);
        if state == 0 {
            let mut transaction = self.history_database.begin_write()?;
            transaction.set_durability(Durability::Immediate)?;
            { let mut meta=transaction.open_table(META)?;meta.insert(&HISTORY_SEQUENCE,&0)?;meta.insert(&HISTORY_EPOCH,&0)?;meta.insert(&HISTORY_BINARY_MIGRATION,&1)?; }
            transaction.commit()?;
            return Ok((0,0));
        }
        self.history_progress()
    }

    pub fn complete_binary_history_migration(&self) -> Result<()> {
        let mut transaction=self.history_database.begin_write()?;transaction.set_durability(Durability::Immediate)?;
        {transaction.open_table(META)?.insert(&HISTORY_BINARY_MIGRATION,&2)?;}transaction.commit()?;Ok(())
    }

    pub fn materialize_history(&self, sequence: u64, epoch: u64, operations: &[Operation]) -> Result<()> {
        let mut transaction = self.history_database.begin_write()?;
        transaction.set_durability(Durability::None)?;
        {
            let mut history = transaction.open_table(HISTORY)?;
            let mut by_owner = transaction.open_table(BY_OWNER)?;
            let mut by_sku = transaction.open_table(BY_SKU)?;
            for operation in operations {
                let key: [u8; 32] = sha2::Sha256::digest(operation.operation_id.as_bytes()).into();
                // The live writer materializes history before the sequential
                // history checkpoint reaches this record. Do not rewrite the
                // payload and both secondary indexes during tail confirmation.
                let already_binary = history.get(key.as_slice())?
                    .is_some_and(|value| value.value().starts_with(b"WOP1"));
                if already_binary { continue; }
                let payload = binary::encode_operation(operation)?;
                history.insert(key.as_slice(), payload.as_slice())?;
                let owner_key = index_key(&operation.owner_id, &operation.operation_id);
                let sku_key = index_key(&operation.sku, &operation.operation_id);
                by_owner.insert(owner_key.as_slice(), key.as_slice())?;
                by_sku.insert(sku_key.as_slice(), key.as_slice())?;
            }
            let mut meta = transaction.open_table(META)?;
            meta.insert(&HISTORY_SEQUENCE, &sequence)?;
            meta.insert(&HISTORY_EPOCH, &epoch)?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn get(&self, key: &[u8; 32]) -> Result<Option<[u8; 32]>> {
        if self.bloom_ready.load(Ordering::Acquire)
            && !bloom_maybe(&self.bloom.read().expect("bloom lock"), key) { return Ok(None); }
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DEDUP)?;
        let Some(value) = table.get(key.as_slice())? else { return Ok(None); };
        let bytes = value.value();
        anyhow::ensure!(bytes.len() == 32, "invalid disk dedup fingerprint length");
        Ok(Some(bytes.try_into().expect("validated fingerprint length")))
    }

    pub fn insert_many(&self, entries: &[([u8; 32], [u8; 32])]) -> Result<()> {
        if entries.is_empty() { return Ok(()); }
        let mut transaction = self.database.begin_write()?;
        // The quorum log and local WAL are the durability sources on the hot path.
        // Redb is a rebuildable materialized index; an Immediate barrier is issued
        // before a snapshot can make the WAL disposable.
        transaction.set_durability(Durability::None)?;
        {
            let mut table = transaction.open_table(DEDUP)?;
            for (key, value) in entries { table.insert(key.as_slice(), value.as_slice())?; }
        }
        transaction.commit()?;
        let mut bloom = self.bloom.write().expect("bloom lock");
        for (key, _) in entries { bloom_add(&mut bloom, key); }
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        {
            let mut table = transaction.open_table(META)?;
            let barrier = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64;
            table.insert(&0, &barrier)?;
        }
        transaction.commit()?;
        let mut history_transaction = self.history_database.begin_write()?;
        history_transaction.set_durability(Durability::Immediate)?;
        { let mut table = history_transaction.open_table(META)?; let barrier = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64; table.insert(&0, &barrier)?; }
        history_transaction.commit()?;
        Ok(())
    }

    pub fn len(&self) -> Result<u64> {
        let transaction = self.database.begin_read()?;
        Ok(transaction.open_table(DEDUP)?.len()?)
    }

    fn rebuild_bloom_in_background(&self) {
        let database = self.database.clone();
        let bloom = self.bloom.clone();
        let ready = self.bloom_ready.clone();
        std::thread::spawn(move || {
            // Let WAL recovery, quorum catch-up and the HTTP listener complete first.
            // This index is an optimization and must never compete with recovery.
            std::thread::sleep(std::time::Duration::from_secs(30));
            let result = (|| -> Result<Vec<u8>> {
                let transaction = database.begin_read()?;
                let table = transaction.open_table(DEDUP)?;
                let mut rebuilt = vec![0; BLOOM_BYTES];
                for (position, entry) in table.iter()?.enumerate() {
                    let (key, _) = entry?;
                    let bytes = key.value();
                    anyhow::ensure!(bytes.len() == 32, "invalid disk dedup key length");
                    bloom_add(&mut rebuilt, bytes.try_into().expect("validated key length"));
                    if position > 0 && position % 50_000 == 0 {
                        std::thread::sleep(std::time::Duration::from_millis(2));
                    }
                }
                Ok(rebuilt)
            })();
            match result {
                Ok(rebuilt) => {
                    let mut current = bloom.write().expect("bloom lock");
                    for (target, source) in current.iter_mut().zip(rebuilt) { *target |= source; }
                    ready.store(true, Ordering::Release);
                    tracing::info!("disk dedup Bloom filter warmed in background");
                }
                Err(error) => tracing::error!(%error, "disk dedup Bloom warm-up failed; safe disk lookups remain active"),
            }
        });
    }
}

fn index_prefix(value: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(value.len() + 1);
    key.extend_from_slice(value.as_bytes());
    key.push(0);
    key
}

fn index_key(value: &str, operation_id: &str) -> Vec<u8> {
    let mut key = index_prefix(value);
    key.extend_from_slice(operation_id.as_bytes());
    key
}

pub fn quarantine(path: &Path) -> Result<PathBuf> {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let target = path.with_extension(format!("redb.corrupt-{timestamp}"));
    std::fs::rename(path, &target)?;
    Ok(target)
}

fn bloom_positions(key: &[u8; 32], bits: usize) -> [usize; 4] {
    [0, 8, 16, 24].map(|offset| {
        u64::from_le_bytes(key[offset..offset + 8].try_into().expect("hash chunk")) as usize % bits
    })
}

fn bloom_maybe(bloom: &[u8], key: &[u8; 32]) -> bool {
    bloom_positions(key, bloom.len() * 8).into_iter().all(|bit| bloom[bit / 8] & (1 << (bit % 8)) != 0)
}

fn bloom_add(bloom: &mut [u8], key: &[u8; 32]) {
    for bit in bloom_positions(key, bloom.len() * 8) { bloom[bit / 8] |= 1 << (bit % 8); }
}
