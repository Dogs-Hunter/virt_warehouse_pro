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
    Apply {
        operation: Operation,
        persist: bool,
        response: oneshot::Sender<anyhow::Result<ApplyResult>>,
    },
    Dump {
        response: oneshot::Sender<Vec<BalanceSnapshot>>,
    },
}

#[derive(Clone)]
pub struct Store {
    shards: Arc<Vec<mpsc::Sender<Command>>>,
    dedup: DedupShards,
    disk_dedup: DiskDedup,
    published_balances: Arc<RwLock<HashMap<Key, i64>>>,
    disk_persist: mpsc::Sender<Vec<([u8; 32], [u8; 32], Operation)>>,
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
                disk_dedup.insert_operations(&recovered_entries)?;
                recovered_entries.clear();
            }
        }
        disk_dedup.insert_operations(&recovered_entries)?;
        disk_dedup.insert_many(&disk_entries)?;

        let published_balances = initial_balances.iter().flat_map(|shard| shard.iter().map(|(key, value)| (key.clone(), *value))).collect();
        let mut shards = Vec::with_capacity(shard_count);
        for balances in initial_balances {
            let (sender, receiver) = mpsc::channel(4096);
            tokio::spawn(run_shard(receiver, wal.clone(), balances));
            shards.push(sender);
        }
        let dedup: DedupShards = Arc::new(
            (0..DEDUP_SHARDS)
                .map(|_| Mutex::new(HashMap::<[u8; 32], DedupEntry>::new()))
                .collect(),
        );
        let (disk_persist, disk_receiver) = mpsc::channel(1024);
        tokio::spawn(run_disk_persist(disk_receiver, disk_dedup.clone(), dedup.clone()));
        Ok(Self {
            shards: Arc::new(shards),
            dedup,
            disk_dedup,
            published_balances: Arc::new(RwLock::new(published_balances)),
            disk_persist,
        })
    }

    pub async fn preflight_batch(&self, operations: &[Operation]) -> anyhow::Result<Vec<bool>> {
        // Hashing strings is CPU work and does not need the shared dedup lock.
        let prepared = operations.iter().map(|operation| (
            operation_id_hash(&operation.operation_id),
            Fingerprint::from(operation),
        )).collect::<Vec<_>>();
        loop {
            let mut shard_ids = prepared.iter().map(|(key, _)| dedup_shard(key)).collect::<Vec<_>>();
            shard_ids.sort_unstable();
            shard_ids.dedup();
            let mut guards = Vec::with_capacity(shard_ids.len());
            for shard in &shard_ids { guards.push(self.dedup[*shard].lock().await); }
            let mut staged = HashMap::<&str, &Operation>::new();
            let mut new_operations = Vec::with_capacity(operations.len());
            let mut fingerprints = Vec::with_capacity(operations.len());
            let mut pending = None;

            for (operation, (id_hash, fingerprint)) in operations.iter().zip(&prepared) {
                if let Some(existing) = staged.get(operation.operation_id.as_str()) {
                    if Fingerprint::from(*existing) != Fingerprint::from(operation) {
                        return Err(ConflictError(operation.operation_id.clone()).into());
                    }
                    new_operations.push(false);
                    fingerprints.push(None);
                    continue;
                }
                let guard_index = shard_ids.binary_search(&dedup_shard(id_hash)).expect("dedup shard locked");
                match guards[guard_index].get(id_hash) {
                    Some(DedupEntry::Committed(existing)) if existing != fingerprint => return Err(ConflictError(operation.operation_id.clone()).into()),
                    Some(DedupEntry::Committed(_)) => { new_operations.push(false); fingerprints.push(None); }
                    Some(DedupEntry::Pending { fingerprint: existing, notify }) => {
                        if existing.as_ref() != Some(fingerprint) { return Err(ConflictError(operation.operation_id.clone()).into()); }
                        pending=Some(notify.clone());break;
                    }
                    None => match self.disk_dedup.get(id_hash)? {
                        Some(existing) if existing != *fingerprint => return Err(ConflictError(operation.operation_id.clone()).into()),
                        Some(_) => { new_operations.push(false); fingerprints.push(None); }
                        None => { new_operations.push(true); fingerprints.push(Some(*fingerprint)); }
                    }
                }
                staged.insert(operation.operation_id.as_str(), operation);
            }
            if let Some(notify)=pending{let notified=notify.notified();drop(guards);notified.await;continue;}
            let notify=Arc::new(Notify::new());
            for (((_,is_new),fingerprint),(id_hash,_)) in operations.iter().zip(&new_operations).zip(fingerprints).zip(&prepared){if *is_new{let position=shard_ids.binary_search(&dedup_shard(id_hash)).expect("dedup shard locked");guards[position].insert(*id_hash,DedupEntry::Pending{fingerprint,notify:notify.clone()});}}
            return Ok(new_operations);
        }
    }

    pub async fn finish_batch(&self, operations: &[Operation], new_operations: &[bool], committed: bool) {
        let keys = operations.iter().zip(new_operations).filter_map(|(operation, is_new)| is_new.then(|| operation_id_hash(&operation.operation_id))).collect::<Vec<_>>();
        let mut shard_ids = keys.iter().map(dedup_shard).collect::<Vec<_>>();
        shard_ids.sort_unstable();
        shard_ids.dedup();
        let mut guards = Vec::with_capacity(shard_ids.len());
        for shard in &shard_ids { guards.push(self.dedup[*shard].lock().await); }
        let mut notifications = Vec::new();
        let mut disk_entries = Vec::new();
        for (operation, is_new) in operations.iter().zip(new_operations) {
            if !*is_new { continue; }
            let id_hash = operation_id_hash(&operation.operation_id);
            if committed {
                let position = shard_ids.binary_search(&dedup_shard(&id_hash)).expect("dedup shard locked");
                if let Some(entry) = guards[position].get(&id_hash) {
                    if let DedupEntry::Pending { fingerprint, notify } = entry {
                        disk_entries.push((id_hash, fingerprint.expect("pending fingerprint")));
                        notifications.push(notify.clone());
                    }
                }
            } else { let position = shard_ids.binary_search(&dedup_shard(&id_hash)).expect("dedup shard locked"); if let Some(DedupEntry::Pending { notify, .. }) = guards[position].remove(&id_hash) {
                notifications.push(notify);
            }}
        }
        if committed {
            let history_entries = operations.iter().zip(new_operations).filter_map(|(operation, is_new)| {
                if !*is_new { return None; }
                let key = operation_id_hash(&operation.operation_id);
                disk_entries.iter().find(|(candidate, _)| candidate == &key)
                    .map(|(_, fingerprint)| (key, *fingerprint, operation.clone()))
            }).collect::<Vec<_>>();
            for (id_hash,fingerprint) in &disk_entries { let position=shard_ids.binary_search(&dedup_shard(id_hash)).expect("dedup shard locked"); if let Some(entry)=guards[position].get_mut(id_hash){*entry=DedupEntry::Committed(*fingerprint);} }
            let mut published = self.published_balances.write().await;
            for (operation, is_new) in operations.iter().zip(new_operations) {
                if *is_new { *published.entry((operation.owner_id.clone(), operation.sku.clone())).or_default() += operation.delta; }
            }
            drop(published);
            drop(guards);
            if !history_entries.is_empty() && self.disk_persist.send(history_entries).await.is_err() {
                tracing::error!("disk materialization queue stopped; committed entries remain recoverable in WAL and quorum");
            }
        } else { drop(guards); }
        for notify in notifications { notify.notify_waiters(); }
    }

    pub async fn apply_reserved_batch(&self, operations: Vec<Operation>) -> anyhow::Result<Vec<ApplyResult>> {
        futures::future::join_all(operations.into_iter().map(|operation| self.apply_to_shard(operation, true)))
            .await.into_iter().collect()
    }

    pub async fn apply_preflight_batch(&self, operations: Vec<Operation>, new_operations: &[bool]) -> anyhow::Result<Vec<ApplyResult>> {
        futures::future::join_all(operations.into_iter().zip(new_operations.iter().copied()).map(|(operation, is_new)| async move {
            if is_new {
                self.apply_to_shard(operation, true).await
            } else {
                let balance = self.balance(operation.owner_id.clone(), operation.sku.clone()).await?;
                Ok(ApplyResult { operation_id: operation.operation_id, status: ApplyStatus::Duplicate, balance })
            }
        })).await.into_iter().collect()
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
    pub fn history_progress(&self) -> anyhow::Result<(u64, u64)> { self.disk_dedup.history_progress() }
    pub fn prepare_binary_history_migration(&self) -> anyhow::Result<(u64, u64)> { self.disk_dedup.prepare_binary_history_migration() }
    pub fn complete_binary_history_migration(&self) -> anyhow::Result<()> { self.disk_dedup.complete_binary_history_migration() }
    pub fn materialize_history(&self, sequence: u64, epoch: u64, operations: &[Operation]) -> anyhow::Result<()> {
        self.disk_dedup.materialize_history(sequence, epoch, operations)
    }
    pub fn operation(&self, operation_id: &str) -> anyhow::Result<Option<Operation>> { self.disk_dedup.get_operation(&operation_id_hash(operation_id)) }
    pub fn operations(&self, operation_ids: &[String]) -> anyhow::Result<Vec<Operation>> {
        let keys = operation_ids.iter().map(|id| operation_id_hash(id)).collect::<Vec<_>>();
        self.disk_dedup.get_operations(&keys)
    }
    pub fn operations_by_owner(&self, owner: &str, cursor: Option<&str>, limit: usize) -> anyhow::Result<Vec<Operation>> { self.disk_dedup.by_owner(owner, cursor, limit) }
    pub fn operations_by_sku(&self, sku: &str, cursor: Option<&str>, limit: usize) -> anyhow::Result<Vec<Operation>> { self.disk_dedup.by_sku(sku, cursor, limit) }
    pub fn sync_disk_dedup(&self) -> anyhow::Result<()> { self.disk_dedup.sync() }

    async fn apply_to_shard(&self, operation: Operation, persist: bool) -> anyhow::Result<ApplyResult> {
        let shard = shard_for(&operation.owner_id, &operation.sku, self.shards.len());
        let (response, result) = oneshot::channel();
        self.shards[shard]
            .send(Command::Apply { operation, persist, response })
            .await
            .map_err(|_| anyhow::anyhow!("state shard stopped"))?;
        result
            .await
            .map_err(|_| anyhow::anyhow!("state shard dropped response"))?
    }

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

}

async fn run_disk_persist(
    mut receiver: mpsc::Receiver<Vec<([u8; 32], [u8; 32], Operation)>>,
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
        let persisted = combined.iter().map(|(key, fingerprint, _)| (*key, *fingerprint)).collect::<Vec<_>>();
        let disk_clone = disk.clone();
        let write_entries = combined;
        match tokio::task::spawn_blocking(move || disk_clone.insert_operations(&write_entries)).await {
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
    wal: Wal,
    mut balances: HashMap<Key, i64>,
) {
    let mut applied = HashSet::<String>::new();

    while let Some(first) = receiver.recv().await {
        let mut commands = Vec::with_capacity(1024);
        commands.push(first);
        while commands.len() < 1024 {
            match receiver.try_recv() {
                Ok(command) => commands.push(command),
                Err(_) => break,
            }
        }

        let mut staged = HashSet::new();
        let durable = commands
            .iter()
            .filter_map(|command| match command {
                Command::Apply { operation, persist, .. }
                    if *persist && !applied.contains(&operation.operation_id)
                        && staged.insert(operation.operation_id.clone()) =>
                {
                    Some(operation.clone())
                }
                _ => None,
            })
            .collect::<Vec<_>>();

        if let Err(error) = wal.append_many(&durable).await {
            let message = error.to_string();
            for command in commands {
                match command {
                    Command::Apply { response, .. } => {
                        let _ = response.send(Err(anyhow::anyhow!(message.clone())));
                    }
                    Command::Dump { response } => {
                        let _ = response.send(balances.iter().map(|((owner_id, sku), balance)| BalanceSnapshot {
                            owner_id: owner_id.clone(), sku: sku.clone(), balance: *balance,
                        }).collect());
                    }
                }
            }
            continue;
        }

        for command in commands {
            match command {
                Command::Dump { response } => {
                    let _ = response.send(balances.iter().map(|((owner_id, sku), balance)| BalanceSnapshot {
                        owner_id: owner_id.clone(), sku: sku.clone(), balance: *balance,
                    }).collect());
                }
                Command::Apply {
                    operation,
                    persist: _,
                    response,
                } => {
                    let key = (operation.owner_id.clone(), operation.sku.clone());
                    if applied.contains(&operation.operation_id) {
                        let _ = response.send(Ok(ApplyResult {
                            operation_id: operation.operation_id,
                            status: ApplyStatus::Duplicate,
                            balance: *balances.get(&key).unwrap_or(&0),
                        }));
                        continue;
                    }
                    let balance = balances.entry(key).or_default();
                    *balance += operation.delta;
                    applied.insert(operation.operation_id.clone());
                    let _ = response.send(Ok(ApplyResult {
                        operation_id: operation.operation_id,
                        status: ApplyStatus::Applied,
                        balance: *balance,
                    }));
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
