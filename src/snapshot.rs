use std::{io::ErrorKind, path::{Path, PathBuf}, time::{SystemTime, UNIX_EPOCH}};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{fs::OpenOptions, io::AsyncWriteExt};

const MAGIC: &[u8; 8] = b"WHSNAP02";
const LEGACY_MAGIC: &[u8; 8] = b"WHSNAP01";
const HEADER_SIZE: usize = 48;

#[derive(Debug, Deserialize, Serialize)]
pub struct BalanceSnapshot {
    pub owner_id: String,
    pub sku: String,
    pub balance: i64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DedupSnapshot {
    pub operation_id: String,
    pub fingerprint: [u8; 32],
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SnapshotData {
    pub version: u32,
    pub sequence: u64,
    pub balances: Vec<BalanceSnapshot>,
    pub dedup: Vec<DedupSnapshot>,
    #[serde(default)]
    pub writer_epoch: u64,
}

pub async fn load(path: &Path) -> Result<Option<SnapshotData>> {
    let data = match tokio::fs::read(path).await {
        Ok(data) => data,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    anyhow::ensure!(data.len() >= HEADER_SIZE, "snapshot header is truncated");
    let magic: &[u8; 8] = data[..8].try_into()?;
    anyhow::ensure!(magic == MAGIC || magic == LEGACY_MAGIC, "snapshot magic mismatch");
    let length = u64::from_le_bytes(data[8..16].try_into()?) as usize;
    anyhow::ensure!(length <= 4 * 1024 * 1024 * 1024_usize, "snapshot exceeds 4 GiB");
    anyhow::ensure!(data.len() == HEADER_SIZE + length, "snapshot length mismatch");
    let payload = &data[HEADER_SIZE..];
    anyhow::ensure!(Sha256::digest(payload).as_slice() == &data[16..48], "snapshot checksum mismatch");
    let snapshot = if magic == LEGACY_MAGIC {
        serde_json::from_slice(payload).context("invalid legacy snapshot payload")?
    } else {
        decode(payload).context("invalid snapshot payload")?
    };
    anyhow::ensure!((1..=3).contains(&snapshot.version), "unsupported snapshot version {}", snapshot.version);
    Ok(Some(snapshot))
}

pub async fn save(path: &Path, snapshot: &SnapshotData) -> Result<()> {
    let payload = encode(snapshot)?;
    let temporary = path.with_extension("snapshot.tmp");
    let mut file = OpenOptions::new().create(true).truncate(true).write(true).open(&temporary).await?;
    file.write_all(MAGIC).await?;
    file.write_all(&(payload.len() as u64).to_le_bytes()).await?;
    file.write_all(&Sha256::digest(&payload)).await?;
    file.write_all(&payload).await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(&temporary, path).await?;
    if let Some(parent) = path.parent() {
        if let Ok(directory) = OpenOptions::new().read(true).open(parent).await { let _ = directory.sync_all().await; }
    }
    Ok(())
}

fn encode(snapshot: &SnapshotData) -> Result<Vec<u8>> {
    let estimated = snapshot.balances.len().saturating_mul(48)
        .saturating_add(snapshot.dedup.len().saturating_mul(72));
    let mut output = Vec::with_capacity(28_usize.saturating_add(estimated));
    output.extend_from_slice(&snapshot.version.to_le_bytes());
    output.extend_from_slice(&snapshot.sequence.to_le_bytes());
    output.extend_from_slice(&(snapshot.balances.len() as u64).to_le_bytes());
    for entry in &snapshot.balances {
        put_string(&mut output, &entry.owner_id)?;
        put_string(&mut output, &entry.sku)?;
        output.extend_from_slice(&entry.balance.to_le_bytes());
    }
    output.extend_from_slice(&(snapshot.dedup.len() as u64).to_le_bytes());
    for entry in &snapshot.dedup {
        put_string(&mut output, &entry.operation_id)?;
        output.extend_from_slice(&entry.fingerprint);
    }
    if snapshot.version >= 3 { output.extend_from_slice(&snapshot.writer_epoch.to_le_bytes()); }
    Ok(output)
}

fn put_string(output: &mut Vec<u8>, value: &str) -> Result<()> {
    anyhow::ensure!(value.len() <= u16::MAX as usize, "snapshot string is too long");
    output.extend_from_slice(&(value.len() as u16).to_le_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn decode(payload: &[u8]) -> Result<SnapshotData> {
    let mut reader = Reader { data: payload, cursor: 0 };
    let version = reader.u32()?;
    let sequence = reader.u64()?;
    let balance_count = reader.count()?;
    let mut balances = Vec::with_capacity(balance_count);
    for _ in 0..balance_count {
        balances.push(BalanceSnapshot {
            owner_id: reader.string()?,
            sku: reader.string()?,
            balance: reader.i64()?,
        });
    }
    let dedup_count = reader.count()?;
    let mut dedup = Vec::with_capacity(dedup_count);
    for _ in 0..dedup_count {
        let operation_id = reader.string()?;
        let fingerprint = reader.take(32)?.try_into().expect("fixed fingerprint length");
        dedup.push(DedupSnapshot { operation_id, fingerprint });
    }
    let writer_epoch = if version >= 3 { reader.u64()? } else { 0 };
    anyhow::ensure!(reader.cursor == payload.len(), "snapshot contains trailing data");
    Ok(SnapshotData { version, sequence, balances, dedup, writer_epoch })
}

struct Reader<'a> {
    data: &'a [u8],
    cursor: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self.cursor.checked_add(length).context("snapshot offset overflow")?;
        anyhow::ensure!(end <= self.data.len(), "snapshot is truncated");
        let value = &self.data[self.cursor..end];
        self.cursor = end;
        Ok(value)
    }

    fn u16(&mut self) -> Result<u16> { Ok(u16::from_le_bytes(self.take(2)?.try_into()?)) }
    fn u32(&mut self) -> Result<u32> { Ok(u32::from_le_bytes(self.take(4)?.try_into()?)) }
    fn u64(&mut self) -> Result<u64> { Ok(u64::from_le_bytes(self.take(8)?.try_into()?)) }
    fn i64(&mut self) -> Result<i64> { Ok(i64::from_le_bytes(self.take(8)?.try_into()?)) }

    fn count(&mut self) -> Result<usize> {
        let count = usize::try_from(self.u64()?).context("snapshot count does not fit memory")?;
        anyhow::ensure!(count <= 100_000_000, "snapshot item count is unreasonable");
        Ok(count)
    }

    fn string(&mut self) -> Result<String> {
        let length = self.u16()? as usize;
        Ok(std::str::from_utf8(self.take(length)?)?.to_owned())
    }
}

pub async fn quarantine(path: &Path) -> Result<PathBuf> {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let quarantined = path.with_extension(format!("snapshot.corrupt-{timestamp}"));
    tokio::fs::rename(path, &quarantined).await
        .with_context(|| format!("cannot quarantine damaged snapshot as {}", quarantined.display()))?;
    Ok(quarantined)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("warehouse-{name}-{}-{}", std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()))
    }

    #[tokio::test]
    async fn snapshot_round_trip_preserves_state() {
        let file = path("snapshot");
        let value = SnapshotData {
            version: 3,
            sequence: 42,
            balances: vec![BalanceSnapshot { owner_id: "owner".into(), sku: "sku".into(), balance: 77 }],
            dedup: vec![DedupSnapshot { operation_id: "op-1".into(), fingerprint: [5; 32] }],
            writer_epoch: 8,
        };
        save(&file, &value).await.unwrap();
        let loaded = load(&file).await.unwrap().unwrap();
        assert_eq!(loaded.sequence, 42);
        assert_eq!(loaded.writer_epoch, 8);
        assert_eq!(loaded.balances[0].balance, 77);
        assert_eq!(loaded.dedup[0].fingerprint, [5; 32]);
        let _ = tokio::fs::remove_file(file).await;
    }

    #[tokio::test]
    async fn snapshot_checksum_detects_corruption() {
        let file = path("snapshot-corrupt");
        let value = SnapshotData { version: 3, sequence: 1, balances: Vec::new(), dedup: Vec::new(), writer_epoch: 1 };
        save(&file, &value).await.unwrap();
        let mut bytes = tokio::fs::read(&file).await.unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        tokio::fs::write(&file, bytes).await.unwrap();
        assert!(load(&file).await.is_err());
        let _ = tokio::fs::remove_file(file).await;
    }
}
