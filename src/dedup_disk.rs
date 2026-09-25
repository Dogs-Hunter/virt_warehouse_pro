use std::{path::{Path, PathBuf}, sync::{Arc, RwLock, atomic::{AtomicBool, Ordering}}, time::{SystemTime, UNIX_EPOCH}};

use anyhow::{Context, Result};
use fjall::{Database as LsmDatabase, Keyspace, KeyspaceCreateOptions, PersistMode};
use redb::{Builder, Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use sha2::Digest;
use crate::{binary, model::Operation};

const DEDUP: TableDefinition<&[u8], &[u8]> = TableDefinition::new("dedup_v1");
const META: TableDefinition<u8, u64> = TableDefinition::new("dedup_meta_v1");
const HISTORY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("operation_history_v1");
const BY_OWNER: TableDefinition<&[u8], &[u8]> = TableDefinition::new("operation_by_owner_v1");
const BY_SKU: TableDefinition<&[u8], &[u8]> = TableDefinition::new("operation_by_sku_v1");
// At tens of millions of committed IDs, 32 MiB produced enough false
// positives to send dozens of brand-new IDs from every request into the
// large compatibility trees. 112 MiB keeps almost all negative checks
// memory-only while leaving headroom for transient compaction buffers.
const BLOOM_BYTES: usize = 112 * 1024 * 1024;
const LSM_CACHE_BYTES: u64 = 16 * 1024 * 1024;
const LSM_MAX_JOURNAL_BYTES: u64 = 256 * 1024 * 1024;
const LSM_MEMTABLE_BYTES: u64 = 16 * 1024 * 1024;
const DEDUP_LSM_SHARDS: usize = 64;
const DEDUP_SHARD_MEMTABLE_BYTES: u64 = 4 * 1024 * 1024;
const LSM_WORKER_THREADS: usize = 8;
// redb is now a read-only compatibility source. Large caches here duplicated
// Fjall and OS caches on every application replica without serving the hot path.
const DEDUP_CACHE_BYTES: usize = 16 * 1024 * 1024;
const DEDUP_SHARDS: usize = 16;
const DEDUP_SHARD_CACHE_BYTES: usize = DEDUP_CACHE_BYTES / DEDUP_SHARDS;
const HISTORY_CACHE_BYTES: usize = 16 * 1024 * 1024;
const HISTORY_SEQUENCE: u8 = 1;
const HISTORY_EPOCH: u8 = 2;
const HISTORY_BINARY_MIGRATION: u8 = 3;
const HISTORY_LSM_ID_FORMAT: u8 = 4;

#[derive(Clone)]
pub struct DiskDedup {
    lsm_database: Arc<LsmDatabase>,
    // New writes are spread across small independent trees. The original
    // monolithic tree remains a read-only compatibility source.
    lsm_dedup: Keyspace,
    lsm_dedup_shards: Arc<Vec<Keyspace>>,
    lsm_history: Keyspace,
    lsm_by_owner: Keyspace,
    lsm_by_sku: Keyspace,
    lsm_history_meta: Keyspace,
    // Kept as a read-only compatibility source for installations created
    // before the disk index was segmented.
    database: Arc<Database>,
    databases: Arc<Vec<Arc<Database>>>,
    history_database: Arc<Database>,
    legacy_fallback: bool,
    bloom: Arc<RwLock<Vec<u8>>>,
    bloom_ready: Arc<AtomicBool>,
}

impl DiskDedup {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
        let existing_dedup = path.exists();
        let lsm_path = path.with_file_name("dedup-lsm");
        let existing_lsm = lsm_path.exists();
        let lsm_database = LsmDatabase::builder(&lsm_path)
            .cache_size(LSM_CACHE_BYTES)
            .max_journaling_size(LSM_MAX_JOURNAL_BYTES)
            .worker_threads(LSM_WORKER_THREADS)
            .open()
            .context("cannot open LSM dedup index")?;
        let point_keyspace = || KeyspaceCreateOptions::default()
            .max_memtable_size(LSM_MEMTABLE_BYTES)
            .expect_point_read_hits(true);
        let range_keyspace = || KeyspaceCreateOptions::default().max_memtable_size(LSM_MEMTABLE_BYTES);
        let lsm_dedup = lsm_database.keyspace("dedup", point_keyspace)
            .context("cannot open LSM dedup keyspace")?;
        let mut lsm_dedup_shards = Vec::with_capacity(DEDUP_LSM_SHARDS);
        for shard in 0..DEDUP_LSM_SHARDS {
            let keyspace = lsm_database.keyspace(
                &format!("dedup_v2_{shard:02}"),
                || KeyspaceCreateOptions::default()
                    .max_memtable_size(DEDUP_SHARD_MEMTABLE_BYTES)
                    .expect_point_read_hits(true),
            ).with_context(|| format!("cannot open LSM dedup shard {shard}"))?;
            lsm_dedup_shards.push(keyspace);
        }
        let lsm_history = lsm_database.keyspace("history", point_keyspace)
            .context("cannot open LSM history keyspace")?;
        let lsm_by_owner = lsm_database.keyspace("history_by_owner", range_keyspace)
            .context("cannot open LSM owner index")?;
        let lsm_by_sku = lsm_database.keyspace("history_by_sku", range_keyspace)
            .context("cannot open LSM SKU index")?;
        let lsm_history_meta = lsm_database.keyspace("history_meta", range_keyspace)
            .context("cannot open LSM history metadata")?;
        let database = Builder::new().set_cache_size(DEDUP_SHARD_CACHE_BYTES).create(path)
            .context("cannot open disk dedup index")?;
        let mut databases = Vec::with_capacity(DEDUP_SHARDS);
        for shard in 0..DEDUP_SHARDS {
            let shard_path = dedup_shard_path(path, shard);
            let shard_database = Builder::new().set_cache_size(DEDUP_SHARD_CACHE_BYTES).create(&shard_path)
                .with_context(|| format!("cannot open disk dedup shard {}", shard))?;
            let transaction = shard_database.begin_write()?;
            { let _ = transaction.open_table(DEDUP)?; let _ = transaction.open_table(META)?; }
            transaction.commit()?;
            databases.push(Arc::new(shard_database));
        }
        let history_path = path.with_file_name("history.redb");
        let history_database = Builder::new().set_cache_size(HISTORY_CACHE_BYTES).create(&history_path)
            .context("cannot open disk history index")?;
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
        // Compatibility stores are immutable after migration. Remember once
        // whether they contain anything so a normal LSM miss does not open 17
        // empty Redb read transactions for every incoming batch.
        let legacy_fallback = dedup_table_len(&database)? > 0
            || databases.iter().try_fold(false, |found, database| {
                Ok::<_, anyhow::Error>(found || dedup_table_len(database)? > 0)
            })?;
        let instance = Self {
            lsm_database: Arc::new(lsm_database),
            lsm_dedup,
            lsm_dedup_shards: Arc::new(lsm_dedup_shards),
            lsm_history,
            lsm_by_owner,
            lsm_by_sku,
            lsm_history_meta,
            database: Arc::new(database),
            databases: Arc::new(databases),
            history_database: Arc::new(history_database),
            legacy_fallback,
            bloom: Arc::new(RwLock::new(vec![0; BLOOM_BYTES])),
            bloom_ready: Arc::new(AtomicBool::new(!existing_dedup && !existing_lsm)),
        };
        if existing_dedup || existing_lsm { instance.rebuild_bloom_in_background(); }
        Ok(instance)
    }

    pub fn insert_operations(&self, entries: &[([u8; 32], [u8; 32], Operation)]) -> Result<()> {
        if entries.is_empty() { return Ok(()); }
        self.insert_many(&entries.iter().map(|(key, fingerprint, _)| (*key, *fingerprint)).collect::<Vec<_>>())?;
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
        Ok(())
    }

    pub fn by_owner(&self, owner: &str, cursor: Option<&str>, limit: usize) -> Result<Vec<Operation>> {
        self.by_index(&self.lsm_by_owner, BY_OWNER, owner, cursor, limit)
    }

    pub fn by_sku(&self, sku: &str, cursor: Option<&str>, limit: usize) -> Result<Vec<Operation>> {
        self.by_index(&self.lsm_by_sku, BY_SKU, sku, cursor, limit)
    }

    fn by_index(&self, lsm_index: &Keyspace, definition: TableDefinition<&[u8], &[u8]>, value: &str, cursor: Option<&str>, limit: usize) -> Result<Vec<Operation>> {
        let prefix = index_prefix(value);
        let mut end = prefix.clone();
        end.push(0xff);
        let mut result = Vec::with_capacity(limit);
        for entry in lsm_index.range(prefix.as_slice()..end.as_slice()) {
            let (index_key, operation_key) = entry.into_inner()?;
            let id = std::str::from_utf8(&index_key[prefix.len()..])?;
            if cursor.is_some_and(|cursor| id <= cursor) { continue; }
            if let Some(payload) = self.lsm_history.get(operation_key.as_ref())? {
                result.push(binary::decode_operation(&payload)?);
                if result.len() == limit { return Ok(result); }
            }
        }
        // A pre-LSM installation is read from redb until its first backfill has
        // populated the new index.
        if !result.is_empty() || self.lsm_history.len()? > 0 { return Ok(result); }
        let transaction = self.history_database.begin_read()?;
        let index = transaction.open_table(definition)?;
        let history = transaction.open_table(HISTORY)?;
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

    pub fn get_operation(&self, operation_id: &str) -> Result<Option<Operation>> {
        if let Some(value) = self.lsm_history.get(operation_id.as_bytes())? { return Ok(Some(binary::decode_operation(&value)?)); }
        let key: [u8; 32] = sha2::Sha256::digest(operation_id.as_bytes()).into();
        let transaction = self.history_database.begin_read()?;
        let table = transaction.open_table(HISTORY)?;
        let Some(value) = table.get(key.as_slice())? else { return Ok(None); };
        Ok(Some(binary::decode_operation(value.value()).or_else(|_| serde_json::from_slice(value.value()).context("invalid legacy JSON history record"))?))
    }

    pub fn get_operations(&self, operation_ids: &[String]) -> Result<Vec<Operation>> {
        let transaction = self.history_database.begin_read()?;
        let table = transaction.open_table(HISTORY)?;
        let mut result = Vec::with_capacity(operation_ids.len());
        for operation_id in operation_ids {
            if let Some(value) = self.lsm_history.get(operation_id.as_bytes())? {
                result.push(binary::decode_operation(&value)?);
            } else {
                let key: [u8; 32] = sha2::Sha256::digest(operation_id.as_bytes()).into();
                if let Some(value) = table.get(key.as_slice())? {
                result.push(binary::decode_operation(value.value()).or_else(|_| serde_json::from_slice(value.value()).context("invalid legacy JSON history record"))?);
                }
            }
        }
        Ok(result)
    }

    pub fn scan_operations(&self, cursor: Option<&str>, limit: usize) -> Result<Vec<Operation>> {
        let start = cursor.unwrap_or("").as_bytes();
        let mut result = Vec::with_capacity(limit);
        for entry in self.lsm_history.range(start..) {
            let (key, value) = entry.into_inner()?;
            if cursor.is_some_and(|cursor| key.as_ref() == cursor.as_bytes()) { continue; }
            result.push(binary::decode_operation(&value)?);
            if result.len() == limit { break; }
        }
        Ok(result)
    }

    pub fn history_len(&self) -> Result<u64> {
        let lsm = self.lsm_history.len()? as u64;
        if lsm > 0 { return Ok(lsm); }
        let transaction = self.history_database.begin_read()?;
        Ok(transaction.open_table(HISTORY)?.len()?)
    }

    pub fn history_progress(&self) -> Result<(u64, u64)> {
        Ok((
            lsm_u64(&self.lsm_history_meta, HISTORY_SEQUENCE)?,
            lsm_u64(&self.lsm_history_meta, HISTORY_EPOCH)?,
        ))
    }

    pub fn prepare_binary_history_migration(&self) -> Result<(u64, u64)> {
        let state = lsm_u64(&self.lsm_history_meta, HISTORY_LSM_ID_FORMAT)?;
        if state == 0 {
            let mut batch = self.lsm_database.batch();
            batch.insert(&self.lsm_history_meta, [HISTORY_SEQUENCE], 0u64.to_le_bytes());
            batch.insert(&self.lsm_history_meta, [HISTORY_EPOCH], 0u64.to_le_bytes());
            batch.insert(&self.lsm_history_meta, [HISTORY_BINARY_MIGRATION], 1u64.to_le_bytes());
            batch.insert(&self.lsm_history_meta, [HISTORY_LSM_ID_FORMAT], 1u64.to_le_bytes());
            batch.commit()?;
            self.lsm_database.persist(PersistMode::SyncAll)?;
            return Ok((0,0));
        }
        self.history_progress()
    }

    pub fn complete_binary_history_migration(&self) -> Result<()> {
        self.lsm_history_meta.insert([HISTORY_BINARY_MIGRATION], 2u64.to_le_bytes())?;
        self.lsm_history_meta.insert([HISTORY_LSM_ID_FORMAT], 2u64.to_le_bytes())?;
        self.lsm_database.persist(PersistMode::SyncAll)?;
        Ok(())
    }

    pub fn materialize_history_batch(&self, records: &[(u64, u64, Vec<Operation>)]) -> Result<()> {
        if records.is_empty() { return Ok(()); }
        let mut batch = self.lsm_database.batch();
        for operation in records.iter().flat_map(|(_, _, operations)| operations) {
            let key = operation.operation_id.as_bytes().to_vec();
            batch.insert(&self.lsm_history, key.clone(), binary::encode_operation(operation)?);
            batch.insert(&self.lsm_by_owner, index_key(&operation.owner_id, &operation.operation_id), key.clone());
            batch.insert(&self.lsm_by_sku, index_key(&operation.sku, &operation.operation_id), key);
        }
        let (sequence, epoch, _) = records.last().expect("non-empty history batch");
        batch.insert(&self.lsm_history_meta, [HISTORY_SEQUENCE], sequence.to_le_bytes());
        batch.insert(&self.lsm_history_meta, [HISTORY_EPOCH], epoch.to_le_bytes());
        batch.durability(None).commit()?;
        Ok(())
    }

    pub fn get(&self, key: &[u8; 32]) -> Result<Option<[u8; 32]>> {
        if self.bloom_ready.load(Ordering::Acquire)
            && !bloom_maybe(&self.bloom.read().expect("bloom lock"), key) { return Ok(None); }
        if let Some(value) = self.lsm_dedup_shards[dedup_lsm_shard(key)].get(key)? {
            return Ok(Some(fingerprint_bytes(&value)?));
        }
        if let Some(value) = self.lsm_dedup.get(key)? { return Ok(Some(fingerprint_bytes(&value)?)); }
        if !self.legacy_fallback { return Ok(None); }
        if let Some(value) = read_fingerprint(&self.databases[dedup_disk_shard(key)], key)? { return Ok(Some(value)); }
        read_fingerprint(&self.database, key)
    }

    pub fn get_many(&self, keys: &[[u8; 32]]) -> Result<Vec<Option<[u8; 32]>>> {
        let bloom = self.bloom.read().expect("bloom lock");
        let ready = self.bloom_ready.load(Ordering::Acquire);
        let candidates = keys.iter().map(|key| !ready || bloom_maybe(&bloom, key)).collect::<Vec<_>>();
        drop(bloom);
        if !candidates.iter().any(|candidate| *candidate) {
            return Ok(vec![None; keys.len()]);
        }
        let mut result = vec![None; keys.len()];
        for shard in 0..DEDUP_LSM_SHARDS {
            for (index, key) in keys.iter().enumerate() {
                if candidates[index] && dedup_lsm_shard(key) == shard {
                    if let Some(value) = self.lsm_dedup_shards[shard].get(key)? {
                        result[index] = Some(fingerprint_bytes(&value)?);
                    }
                }
            }
        }
        for (index, key) in keys.iter().enumerate() {
            if candidates[index] && result[index].is_none() {
                if let Some(value) = self.lsm_dedup.get(key)? {
                    result[index] = Some(fingerprint_bytes(&value)?);
                }
            }
        }
        if self.legacy_fallback {
            for shard in 0..DEDUP_SHARDS {
                let indices = keys.iter().enumerate().filter_map(|(index, key)|
                    (candidates[index] && result[index].is_none() && dedup_disk_shard(key) == shard).then_some(index)).collect::<Vec<_>>();
                if indices.is_empty() { continue; }
                let transaction = self.databases[shard].begin_read()?;
                let table = transaction.open_table(DEDUP)?;
                for index in indices {
                    if let Some(value) = table.get(keys[index].as_slice())? {
                        result[index] = Some(fingerprint_bytes(value.value())?);
                    }
                }
            }
            // Old installations keep their pre-segmentation records in the
            // legacy file. Only misses need the compatibility lookup.
            let transaction = self.database.begin_read()?;
            let table = transaction.open_table(DEDUP)?;
            for (index, key) in keys.iter().enumerate() {
                if candidates[index] && result[index].is_none() {
                    if let Some(value) = table.get(key.as_slice())? {
                        result[index] = Some(fingerprint_bytes(value.value())?);
                    }
                }
            }
        }
        Ok(result)
    }

    pub fn insert_many(&self, entries: &[([u8; 32], [u8; 32])]) -> Result<()> {
        if entries.is_empty() { return Ok(()); }
        let mut batch = self.lsm_database.batch();
        for (key, value) in entries {
            batch.insert(&self.lsm_dedup_shards[dedup_lsm_shard(key)], key, value);
        }
        // JetStream quorum and the local WAL are the durability sources on the
        // request path. Fjall is flushed at explicit checkpoints.
        batch.durability(None).commit()?;
        let mut bloom = self.bloom.write().expect("bloom lock");
        for (key, _) in entries { bloom_add(&mut bloom, key); }
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.lsm_database.persist(PersistMode::SyncAll)?;
        for database in self.databases.iter() {
            let mut transaction = database.begin_write()?;
            transaction.set_durability(Durability::Immediate)?;
            { let mut table = transaction.open_table(META)?; let barrier = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64; table.insert(&0, &barrier)?; }
            transaction.commit()?;
        }
        let mut history_transaction = self.history_database.begin_write()?;
        history_transaction.set_durability(Durability::Immediate)?;
        { let mut table = history_transaction.open_table(META)?; let barrier = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64; table.insert(&0, &barrier)?; }
        history_transaction.commit()?;
        Ok(())
    }

    pub fn len(&self) -> Result<u64> {
        let legacy = { let transaction = self.database.begin_read()?; transaction.open_table(DEDUP)?.len()? };
        let lsm = self.lsm_dedup.len()? as u64 + self.lsm_dedup_shards.iter()
            .try_fold(0_u64, |total, shard| Ok::<_, anyhow::Error>(total + shard.len()? as u64))?;
        self.databases.iter().try_fold(legacy + lsm, |total, database| {
            let transaction = database.begin_read()?;
            Ok(total + transaction.open_table(DEDUP)?.len()?)
        })
    }

    pub fn memory_metrics(&self) -> (u64, u64, u64, usize) {
        (
            self.lsm_database.cache_size(),
            self.lsm_database.cache_capacity(),
            self.lsm_database.write_buffer_size(),
            self.lsm_database.outstanding_flushes(),
        )
    }

    pub fn bloom_ready(&self) -> bool { self.bloom_ready.load(Ordering::Acquire) }

    fn rebuild_bloom_in_background(&self) {
        let database = self.database.clone();
        let databases = self.databases.clone();
        let lsm_dedup = self.lsm_dedup.clone();
        let lsm_dedup_shards = self.lsm_dedup_shards.clone();
        let bloom = self.bloom.clone();
        let ready = self.bloom_ready.clone();
        std::thread::spawn(move || {
            // Startup and recovery stay available while this performance-only
            // index is rebuilt. New keys are ORed into the completed image so
            // writes accepted during warm-up cannot produce false negatives.
            let result = (|| -> Result<Vec<u8>> {
                let mut rebuilt = vec![0; BLOOM_BYTES];
                for entry in lsm_dedup.iter() {
                    let key = entry.key()?;
                    anyhow::ensure!(key.len() == 32, "invalid LSM dedup key length");
                    bloom_add(&mut rebuilt, key.as_ref().try_into().expect("validated key length"));
                }
                for shard in lsm_dedup_shards.iter() {
                    for entry in shard.iter() {
                        let key = entry.key()?;
                        anyhow::ensure!(key.len() == 32, "invalid sharded LSM dedup key length");
                        bloom_add(&mut rebuilt, key.as_ref().try_into().expect("validated key length"));
                    }
                }
                for source in std::iter::once(&database).chain(databases.iter()) {
                    let transaction = source.begin_read()?;
                    let table = transaction.open_table(DEDUP)?;
                    for entry in table.iter()? {
                        let (key, _) = entry?;
                        let bytes = key.value();
                        anyhow::ensure!(bytes.len() == 32, "invalid disk dedup key length");
                        bloom_add(&mut rebuilt, bytes.try_into().expect("validated key length"));
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

fn dedup_disk_shard(key: &[u8; 32]) -> usize {
    key[0] as usize & (DEDUP_SHARDS - 1)
}

fn dedup_lsm_shard(key: &[u8; 32]) -> usize {
    key[0] as usize & (DEDUP_LSM_SHARDS - 1)
}

fn dedup_shard_path(path: &Path, shard: usize) -> PathBuf {
    let stem = path.file_stem().and_then(|value| value.to_str()).unwrap_or("dedup");
    path.with_file_name(format!("{stem}.shard-{shard:02}.redb"))
}

fn fingerprint_bytes(bytes: &[u8]) -> Result<[u8; 32]> {
    anyhow::ensure!(bytes.len() == 32, "invalid disk dedup fingerprint length");
    Ok(bytes.try_into().expect("validated fingerprint length"))
}

fn lsm_u64(keyspace: &Keyspace, key: u8) -> Result<u64> {
    let Some(value) = keyspace.get([key])? else { return Ok(0); };
    anyhow::ensure!(value.len() == 8, "invalid LSM metadata value");
    Ok(u64::from_le_bytes(value.as_ref().try_into().expect("validated metadata length")))
}

fn read_fingerprint(database: &Database, key: &[u8; 32]) -> Result<Option<[u8; 32]>> {
    let transaction = database.begin_read()?;
    let table = transaction.open_table(DEDUP)?;
    Ok(table.get(key.as_slice())?.map(|value| fingerprint_bytes(value.value())).transpose()?)
}

fn dedup_table_len(database: &Database) -> Result<u64> {
    let transaction = database.begin_read()?;
    Ok(transaction.open_table(DEDUP)?.len()?)
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
