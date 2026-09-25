use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    sync::Arc,
};

use tokio::sync::{mpsc, oneshot, Mutex, Notify, RwLock};
use sha2::{Digest, Sha256};

use crate::{
    dedup_disk::DiskDedup,
    model::{ApplyResult, ApplyStatus, Operation},
    snapshot::{BalanceSnapshot, DedupSnapshot, SnapshotData},
    wal::Wal,
};

type Key = (String, String);

type Fingerprint = [u8; 32];
const DEDUP_SHARDS: usize = 64;
type DedupShards = Arc<Vec<Mutex<HashMap<[u8; 32], DedupEntry>>>>;

fn dedup_shard(key: &[u8; 32]) -> usize {
    u16::from_le_bytes([key[0], key[1]]) as usize % DEDUP_SHARDS
}

#[derive(Debug)]
enum DedupEntry {
    Committed(Fingerprint),
    Pending {
        fingerprint: Option<Fingerprint>,
        notify: Arc<Notify>,
    },
}

impl From<&Operation> for Fingerprint {
    fn from(value: &Operation) -> Self {
        let mut hasher = Sha256::new();
        hash_field(&mut hasher, value.owner_id.as_bytes());
        hash_field(&mut hasher, value.sku.as_bytes());
        hash_field(&mut hasher, &value.delta.to_le_bytes());
        hash_field(&mut hasher, &value.event_version.to_le_bytes());
        hasher.finalize().into()
    }
}

fn hash_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_le_bytes());
    hasher.update(value);
}

#[derive(Debug)]
pub struct ConflictError(pub String);

impl std::fmt::Display for ConflictError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(formatter, "conflicting operation_id: {}", self.0) }
}
impl std::error::Error for ConflictError {}

enum Command {
    ApplyBatch {
        operations: Vec<(usize, Operation)>,
        response: oneshot::Sender<anyhow::Result<Vec<(usize, ApplyResult)>>>,
    },
    Dump {
        response: oneshot::Sender<Vec<BalanceSnapshot>>,
    },
}

struct PublishCommand {
    updates: Vec<(Key, i64)>,
    completed: oneshot::Sender<()>,
}

#[derive(Clone)]
pub struct Store {
    shards: Arc<Vec<mpsc::Sender<Command>>>,
    wal: Wal,
    dedup: DedupShards,
    disk_dedup: DiskDedup,
    published_balances: Arc<RwLock<HashMap<Key, i64>>>,
    balance_publisher: mpsc::Sender<PublishCommand>,
    disk_persist: mpsc::Sender<Vec<([u8; 32], [u8; 32])>>,
}

impl Store {
    pub fn start(shard_count: usize, wal: Wal, recovered: Vec<Operation>, snapshot: Option<SnapshotData>, disk_dedup: DiskDedup) -> anyhow::Result<Self> {
        let mut disk_entries = Vec::with_capacity(100_000);
        let mut initial_balances = (0..shard_count)
            .map(|_| HashMap::new())
            .collect::<Vec<HashMap<Key, i64>>>();
        if let Some(snapshot) = snapshot {
            for entry in snapshot.dedup {
                disk_entries.push((operation_id_hash(&entry.operation_id), entry.fingerprint));
                if disk_entries.len() >= 100_000 {
                    disk_dedup.insert_many(&disk_entries)?;
                    disk_entries.clear();
                }
            }
            for entry in snapshot.balances {
                let shard = shard_for(&entry.owner_id, &entry.sku, shard_count);
                initial_balances[shard].insert((entry.owner_id, entry.sku), entry.balance);
            }
        }
        let mut recovered_entries = Vec::with_capacity(100_000);
        // WAL contains only the suffix after the persisted snapshot. Every unique
        // record in that suffix must rebuild the balance even when the global
        // dedup index was flushed before the snapshot itself was advanced.
        let mut recovered_ids = HashSet::new();
        for operation in recovered {
            let id_hash = operation_id_hash(&operation.operation_id);
            if !recovered_ids.insert(id_hash) { continue; }
            recovered_entries.push((id_hash, Fingerprint::from(&operation), operation.clone()));
            let shard = shard_for(&operation.owner_id, &operation.sku, shard_count);
            *initial_balances[shard].entry((operation.owner_id, operation.sku)).or_default() += operation.delta;
            if recovered_entries.len() >= 100_000 {
                let entries = recovered_entries.iter().map(|(key, fingerprint, _)| (*key, *fingerprint)).collect::<Vec<_>>();
                disk_dedup.insert_many(&entries)?;
                recovered_entries.clear();
            }
        }
        let recovered_dedup = recovered_entries.iter().map(|(key, fingerprint, _)| (*key, *fingerprint)).collect::<Vec<_>>();
        disk_dedup.insert_many(&recovered_dedup)?;
        disk_dedup.insert_many(&disk_entries)?;

        let published_balances = Arc::new(RwLock::new(initial_balances.iter().flat_map(|shard| shard.iter().map(|(key, value)| (key.clone(), *value))).collect()));
        let (balance_publisher, balance_receiver) = mpsc::channel(4096);
        tokio::spawn(run_balance_publisher(balance_receiver, published_balances.clone()));
        let mut shards = Vec::with_capacity(shard_count);
        for balances in initial_balances {
            let (sender, receiver) = mpsc::channel(4096);
            tokio::spawn(run_shard(receiver, balances));
            shards.push(sender);
        }
        let dedup: DedupShards = Arc::new(
            (0..DEDUP_SHARDS)
                .map(|_| Mutex::new(HashMap::<[u8; 32], DedupEntry>::new()))
                .collect(),
        );
        // Bound unmaterialized history. At high sustained ingest Redb can be
        // slower than the quorum/WAL path; a small queue applies backpressure
        // instead of retaining millions of cloned operations in RAM.
        let (disk_persist, disk_receiver) = mpsc::channel(128);
        tokio::spawn(run_disk_persist(disk_receiver, disk_dedup.clone(), dedup.clone()));
        Ok(Self {
            shards: Arc::new(shards),
            wal,
            dedup,
            disk_dedup,
            published_balances,
            balance_publisher,
            disk_persist,
        })
    }

    pub async fn preflight_batch(&self, operations: &[Operation]) -> anyhow::Result<Vec<bool>> {
        let prepared = operations.iter().map(|operation| (
            operation_id_hash(&operation.operation_id),
            Fingerprint::from(operation),
        )).collect::<Vec<_>>();

        // Validate duplicates inside the request before reserving shared state.
        let mut first = HashMap::<&str, ([u8; 32], usize)>::new();
        let mut unique = Vec::with_capacity(operations.len());
        for (index, operation) in operations.iter().enumerate() {
            match first.get(operation.operation_id.as_str()) {
                Some((fingerprint, _)) if fingerprint != &prepared[index].1 => {
                    return Err(ConflictError(operation.operation_id.clone()).into());
                }
                Some(_) => {}
                None => {
                    first.insert(operation.operation_id.as_str(), (prepared[index].1, index));
                    unique.push(index);
                }
            }
        }

        loop {
            let reservation = Arc::new(Notify::new());
            let mut new_operations = vec![false; operations.len()];
            let mut reserved = Vec::with_capacity(unique.len());
            let mut wait_for = None;
            let mut conflict = None;

            for shard_id in 0..DEDUP_SHARDS {
                let indices = unique.iter().copied()
                    .filter(|index| dedup_shard(&prepared[*index].0) == shard_id)
                    .collect::<Vec<_>>();
                if indices.is_empty() { continue; }
                let mut memory = self.dedup[shard_id].lock().await;
                let keys = indices.iter().map(|index| prepared[*index].0).collect::<Vec<_>>();
                let disk_values = self.disk_dedup.get_many(&keys)?;
                for (index, disk_value) in indices.into_iter().zip(disk_values) {
                    let (id_hash, fingerprint) = prepared[index];
                    match memory.get(&id_hash) {
                        Some(DedupEntry::Committed(existing)) if existing != &fingerprint => {
                            conflict = Some(operations[index].operation_id.clone());
                            break;
                        }
                        Some(DedupEntry::Committed(_)) => {}
                        Some(DedupEntry::Pending { fingerprint: existing, notify }) => {
                            if existing.as_ref() != Some(&fingerprint) {
                                conflict = Some(operations[index].operation_id.clone());
                            } else {
                                wait_for = Some(notify.clone().notified_owned());
                            }
                            break;
                        }
                        None => match disk_value {
                            Some(existing) if existing != fingerprint => {
                                conflict = Some(operations[index].operation_id.clone());
                                break;
                            }
                            Some(_) => {}
                            None => {
                                memory.insert(id_hash, DedupEntry::Pending {
                                    fingerprint: Some(fingerprint),
                                    notify: reservation.clone(),
                                });
                                new_operations[index] = true;
                                reserved.push(id_hash);
                            }
                        }
                    }
                }
                drop(memory);
                if conflict.is_some() || wait_for.is_some() { break; }
            }

            if let Some(notified) = wait_for {
                self.rollback_reservation(&reserved, &reservation).await;
                notified.await;
                continue;
            }
            if let Some(operation_id) = conflict {
                self.rollback_reservation(&reserved, &reservation).await;
                return Err(ConflictError(operation_id).into());
            }
            return Ok(new_operations);
        }
    }

    async fn rollback_reservation(&self, keys: &[[u8; 32]], reservation: &Arc<Notify>) {
        for shard_id in 0..DEDUP_SHARDS {
            let shard_keys = keys.iter().filter(|key| dedup_shard(key) == shard_id).copied().collect::<Vec<_>>();
            if shard_keys.is_empty() { continue; }
            let mut memory = self.dedup[shard_id].lock().await;
            for key in shard_keys {
                if matches!(memory.get(&key), Some(DedupEntry::Pending { notify, .. }) if Arc::ptr_eq(notify, reservation)) {
                    memory.remove(&key);
                }
            }
        }
        reservation.notify_waiters();
    }

    pub async fn finish_batch(&self, operations: &[Operation], new_operations: &[bool], committed: bool) {
        let mut notifications = Vec::new();
        let mut disk_entries = Vec::new();
        let prepared = operations.iter().zip(new_operations).map(|(operation, is_new)| {
            is_new.then(|| operation_id_hash(&operation.operation_id))
        }).collect::<Vec<_>>();
        let mut grouped = (0..DEDUP_SHARDS).map(|_| Vec::new()).collect::<Vec<Vec<usize>>>();
        for (index, key) in prepared.iter().enumerate() {
            if let Some(key) = key { grouped[dedup_shard(key)].push(index); }
        }
        for shard_id in 0..DEDUP_SHARDS {
            if grouped[shard_id].is_empty() { continue; }
            let mut memory = self.dedup[shard_id].lock().await;
            for index in &grouped[shard_id] {
                let id_hash = prepared[*index].expect("new operation key");
                if committed {
                    if let Some(DedupEntry::Pending { fingerprint, notify }) = memory.get(&id_hash) {
                        let fingerprint = fingerprint.expect("pending fingerprint");
                        disk_entries.push((id_hash, fingerprint));
                        notifications.push(notify.clone());
                        memory.insert(id_hash, DedupEntry::Committed(fingerprint));
                    }
                } else if let Some(DedupEntry::Pending { notify, .. }) = memory.remove(&id_hash) {
                    notifications.push(notify);
                }
            }
        }
        if committed {
            let updates = operations.iter().zip(new_operations).filter_map(|(operation, is_new)| {
                is_new.then(|| ((operation.owner_id.clone(), operation.sku.clone()), operation.delta))
            }).collect::<Vec<_>>();
            if !updates.is_empty() {
                let (completed, response) = oneshot::channel();
                if self.balance_publisher.send(PublishCommand { updates, completed }).await.is_err() || response.await.is_err() {
                    tracing::error!("balance publisher stopped; committed state remains recoverable in WAL and quorum");
                }
            }
            if !disk_entries.is_empty() && self.disk_persist.send(disk_entries).await.is_err() {
                tracing::error!("disk materialization queue stopped; committed entries remain recoverable in WAL and quorum");
            }
        }
        for notify in notifications { notify.notify_waiters(); }
    }

    pub async fn apply_reserved_batch(&self, operations: Vec<Operation>) -> anyhow::Result<Vec<ApplyResult>> {
        let mut indexed = self.apply_batched(operations.into_iter().enumerate().collect(), true).await?;
        indexed.sort_by_key(|(index, _)| *index);
        Ok(indexed.into_iter().map(|(_, result)| result).collect())
    }

    pub async fn apply_preflight_batch(&self, operations: Vec<Operation>, new_operations: &[bool]) -> anyhow::Result<Vec<ApplyResult>> {
        let total = operations.len();
        let mut results = (0..total).map(|_| None).collect::<Vec<Option<ApplyResult>>>();
        let mut fresh = Vec::new();
        {
            let published = self.published_balances.read().await;
            for (index, (operation, is_new)) in operations.into_iter().zip(new_operations.iter().copied()).enumerate() {
                if is_new {
                    fresh.push((index, operation));
                } else {
                    let balance = *published.get(&(operation.owner_id.clone(), operation.sku.clone())).unwrap_or(&0);
                    results[index] = Some(ApplyResult { operation_id: operation.operation_id, status: ApplyStatus::Duplicate, balance });
                }
            }
        }
        for (index, result) in self.apply_batched(fresh, true).await? {
            results[index] = Some(result);
        }
        results.into_iter().map(|result| result.ok_or_else(|| anyhow::anyhow!("missing state shard result"))).collect()
    }

    async fn apply_batched(&self, indexed: Vec<(usize, Operation)>, persist: bool) -> anyhow::Result<Vec<(usize, ApplyResult)>> {
        if persist {
            let durable = indexed.iter().map(|(_, operation)| operation.clone()).collect::<Vec<_>>();
            self.wal.append_many(&durable).await?;
        }
        let mut groups = (0..self.shards.len()).map(|_| Vec::new()).collect::<Vec<Vec<(usize, Operation)>>>();
        for (index, operation) in indexed {
            let shard = shard_for(&operation.owner_id, &operation.sku, self.shards.len());
            groups[shard].push((index, operation));
        }
        let replies = futures::future::join_all(groups.into_iter().enumerate().filter_map(|(shard, operations)| {
            if operations.is_empty() { return None; }
            let sender = self.shards[shard].clone();
            Some(async move {
                let (response, result) = oneshot::channel();
                sender.send(Command::ApplyBatch { operations, response }).await
                    .map_err(|_| anyhow::anyhow!("state shard stopped"))?;
                result.await.map_err(|_| anyhow::anyhow!("state shard dropped batch response"))?
            })
        })).await;
        let mut completed = Vec::new();
        for reply in replies {
            completed.extend(reply?);
        }
        Ok(completed)
    }

    pub async fn snapshot(&self, sequence: u64, writer_epoch: u64) -> anyhow::Result<SnapshotData> {
        let dedup = Vec::<DedupSnapshot>::new();
        let replies = futures::future::join_all(self.shards.iter().map(|shard| async move {
            let (response, result) = oneshot::channel();
            shard.send(Command::Dump { response }).await.map_err(|_| anyhow::anyhow!("state shard stopped"))?;
            result.await.map_err(|_| anyhow::anyhow!("state shard dropped snapshot response"))
        })).await;
        let mut balances = Vec::new();
        for reply in replies { balances.extend(reply?); }
        Ok(SnapshotData { version: 3, sequence, balances, dedup, writer_epoch })
    }

    pub fn disk_dedup_len(&self) -> anyhow::Result<u64> { self.disk_dedup.len() }
    pub fn history_len(&self) -> anyhow::Result<u64> { self.disk_dedup.history_len() }
    pub fn disk_memory_metrics(&self) -> (u64, u64, u64, usize) { self.disk_dedup.memory_metrics() }
    pub fn disk_bloom_ready(&self) -> bool { self.disk_dedup.bloom_ready() }
    pub async fn runtime_cardinality(&self) -> (usize, usize, usize) {
        let mut committed = 0;
        let mut pending = 0;
        for shard in self.dedup.iter() {
            for entry in shard.lock().await.values() {
                match entry { DedupEntry::Committed(_) => committed += 1, DedupEntry::Pending { .. } => pending += 1 }
            }
        }
        (committed, pending, self.published_balances.read().await.len())
    }
    pub fn history_progress(&self) -> anyhow::Result<(u64, u64)> { self.disk_dedup.history_progress() }
    pub fn prepare_binary_history_migration(&self) -> anyhow::Result<(u64, u64)> { self.disk_dedup.prepare_binary_history_migration() }
    pub fn complete_binary_history_migration(&self) -> anyhow::Result<()> { self.disk_dedup.complete_binary_history_migration() }
    pub fn materialize_history_batch(&self, records: &[(u64, u64, Vec<Operation>)]) -> anyhow::Result<()> {
        self.disk_dedup.materialize_history_batch(records)
    }
    pub fn operation(&self, operation_id: &str) -> anyhow::Result<Option<Operation>> { self.disk_dedup.get_operation(operation_id) }
    pub fn operations(&self, operation_ids: &[String]) -> anyhow::Result<Vec<Operation>> { self.disk_dedup.get_operations(operation_ids) }
    pub fn scan_operations(&self, cursor: Option<&str>, limit: usize) -> anyhow::Result<Vec<Operation>> { self.disk_dedup.scan_operations(cursor, limit) }
    pub fn operations_by_owner(&self, owner: &str, cursor: Option<&str>, limit: usize) -> anyhow::Result<Vec<Operation>> { self.disk_dedup.by_owner(owner, cursor, limit) }
    pub fn operations_by_sku(&self, sku: &str, cursor: Option<&str>, limit: usize) -> anyhow::Result<Vec<Operation>> { self.disk_dedup.by_sku(sku, cursor, limit) }
    pub fn sync_disk_dedup(&self) -> anyhow::Result<()> { self.disk_dedup.sync() }

    pub async fn restore_chunk(&self, operations: Vec<Operation>) -> anyhow::Result<()> {
        let new_operations = self.preflight_batch(&operations).await?;
        let missing = operations.iter().zip(&new_operations)
            .filter_map(|(operation, is_new)| (*is_new).then_some(operation.clone()))
            .collect::<Vec<_>>();
        match self.apply_reserved_batch(missing).await {
            Ok(_) => {
                self.finish_batch(&operations, &new_operations, true).await;
                Ok(())
            }
            Err(error) => {
                self.finish_batch(&operations, &new_operations, false).await;
                Err(error)
            }
        }
    }

    pub async fn balance(&self, owner_id: String, sku: String) -> anyhow::Result<i64> {
        Ok(*self.published_balances.read().await.get(&(owner_id, sku)).unwrap_or(&0))
    }

    pub async fn balances_by_owner(&self, owner_id: &str) -> Vec<(String, i64)> {
        let published = self.published_balances.read().await;
        let mut values = published.iter().filter_map(|((owner, sku), balance)| (owner == owner_id).then(|| (sku.clone(), *balance))).collect::<Vec<_>>();
        values.sort_by(|left, right| left.0.cmp(&right.0));
        values
    }

    pub async fn balances(&self, keys: &[(String, String)]) -> Vec<i64> {
        let published = self.published_balances.read().await;
        keys.iter().map(|key| *published.get(key).unwrap_or(&0)).collect()
    }

}

async fn run_balance_publisher(
    mut receiver: mpsc::Receiver<PublishCommand>,
    balances: Arc<RwLock<HashMap<Key, i64>>>,
) {
    while let Some(first) = receiver.recv().await {
        let mut batch = vec![first];
        // Give concurrently finishing requests one scheduler turn to join this
        // publication without imposing a fixed latency window.
        tokio::task::yield_now().await;
        while batch.len() < 256 {
            match receiver.try_recv() {
                Ok(command) => batch.push(command),
                Err(_) => break,
            }
        }
        {
            let mut published = balances.write().await;
            for command in &batch {
                for (key, delta) in &command.updates {
                    *published.entry(key.clone()).or_default() += delta;
                }
            }
        }
        for command in batch { let _ = command.completed.send(()); }
    }
}

async fn run_disk_persist(
    mut receiver: mpsc::Receiver<Vec<([u8; 32], [u8; 32])>>,
    disk: DiskDedup,
    dedup: DedupShards,
) {
    while let Some(first) = receiver.recv().await {
        let mut combined = first;
        // A tiny coalescing window turns concurrent HTTP batches into one redb
        // transaction instead of starting a transaction for the first arrival.
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        while combined.len() < 100_000 {
            match receiver.try_recv() {
                Ok(mut next) => combined.append(&mut next),
                Err(_) => break,
            }
        }
        let persisted = combined.clone();
        let disk_clone = disk.clone();
        let mut write_entries = combined;
        // SHA-256 keys are uniformly random. Feeding them to a copy-on-write
        // B-tree in arrival order causes excessive page churn as the index
        // grows; ordered bulk insertion keeps the hot write set sequential.
        write_entries.sort_unstable_by_key(|(key, _)| *key);
        match tokio::task::spawn_blocking(move || disk_clone.insert_many(&write_entries)).await {
            Ok(Ok(())) => {
                for (key, fingerprint) in persisted {
                    let mut memory=dedup[dedup_shard(&key)].lock().await;
                    if matches!(memory.get(&key),Some(DedupEntry::Committed(existing)) if *existing==fingerprint) {
                        memory.remove(&key);
                    }
                }
            }
            Ok(Err(error)) => tracing::error!(%error, "grouped disk materialization failed; entries retained in memory"),
            Err(error) => tracing::error!(%error, "grouped disk materialization task failed; entries retained in memory"),
        }
    }
}

async fn run_shard(
    mut receiver: mpsc::Receiver<Command>,
    mut balances: HashMap<Key, i64>,
) {
    while let Some(first) = receiver.recv().await {
        let mut commands = Vec::with_capacity(1024);
        commands.push(first);
        while commands.len() < 1024 {
            match receiver.try_recv() {
                Ok(command) => commands.push(command),
                Err(_) => break,
            }
        }

        for command in commands {
            match command {
                Command::Dump { response } => {
                    let _ = response.send(balances.iter().map(|((owner_id, sku), balance)| BalanceSnapshot {
                        owner_id: owner_id.clone(), sku: sku.clone(), balance: *balance,
                    }).collect());
                }
                Command::ApplyBatch { operations, response } => {
                    let mut results = Vec::with_capacity(operations.len());
                    for (index, operation) in operations {
                        let key = (operation.owner_id.clone(), operation.sku.clone());
                        let balance = balances.entry(key).or_default();
                        *balance += operation.delta;
                        results.push((index, ApplyResult {
                            operation_id: operation.operation_id,
                            status: ApplyStatus::Applied,
                            balance: *balance,
                        }));
                    }
                    let _ = response.send(Ok(results));
                }
            }
        }
    }
}

fn shard_for(owner_id: &str, sku: &str, count: usize) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    owner_id.hash(&mut hasher);
    sku.hash(&mut hasher);
    hasher.finish() as usize & (count - 1)
}

fn operation_id_hash(operation_id: &str) -> [u8; 32] {
    Sha256::digest(operation_id.as_bytes()).into()
}
