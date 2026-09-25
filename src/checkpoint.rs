use std::{path::{Path, PathBuf}, sync::Arc};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::{fs::OpenOptions, io::AsyncWriteExt, sync::Mutex};

#[derive(Clone)]
pub struct Checkpoint {
    path: Arc<PathBuf>,
    value: Arc<Mutex<CheckpointValue>>,
}

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
struct CheckpointValue { sequence: u64, writer_epoch: u64 }

impl Checkpoint {
    pub async fn open(path: &Path) -> Result<Self> {
        let value = match tokio::fs::read(path).await {
            Ok(data) if data.starts_with(b"WCP1") && data.len()==20 => CheckpointValue { sequence:u64::from_le_bytes(data[4..12].try_into()?), writer_epoch:u64::from_le_bytes(data[12..20].try_into()?) },
            Ok(data) => { let text=std::str::from_utf8(&data)?; serde_json::from_str(text).or_else(|_| text.trim().parse().map(|sequence| CheckpointValue { sequence, writer_epoch: 0 }))? },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => CheckpointValue::default(),
            Err(error) => return Err(error.into()),
        };
        Ok(Self { path: Arc::new(path.to_owned()), value: Arc::new(Mutex::new(value)) })
    }

    pub async fn current(&self) -> u64 { self.value.lock().await.sequence }
    pub async fn writer_epoch(&self) -> u64 { self.value.lock().await.writer_epoch }

    pub async fn advance(&self, next: u64, writer_epoch: u64) -> Result<()> {
        let mut current = self.value.lock().await;
        if next <= current.sequence { return Ok(()); }
        let next_value = CheckpointValue { sequence: next, writer_epoch };
        let temporary = self.path.with_extension("checkpoint.tmp");
        let mut file = OpenOptions::new().create(true).truncate(true).write(true).open(&temporary).await?;
        file.write_all(b"WCP1").await?;file.write_all(&next.to_le_bytes()).await?;file.write_all(&writer_epoch.to_le_bytes()).await?;
        file.sync_data().await?;
        tokio::fs::rename(temporary, self.path.as_ref()).await?;
        *current = next_value;
        Ok(())
    }

    pub async fn reset(&self) -> Result<()> {
        let mut current = self.value.lock().await;
        let temporary = self.path.with_extension("checkpoint.tmp");
        let mut file = OpenOptions::new().create(true).truncate(true).write(true).open(&temporary).await?;
        file.write_all(b"WCP1").await?;file.write_all(&0_u64.to_le_bytes()).await?;file.write_all(&0_u64.to_le_bytes()).await?;
        file.sync_data().await?;
        tokio::fs::rename(temporary, self.path.as_ref()).await?;
        *current = CheckpointValue::default();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("warehouse-{name}-{}-{}", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()))
    }

    #[tokio::test]
    async fn checkpoint_is_monotonic_and_survives_reopen() {
        let file = path("checkpoint");
        let checkpoint = Checkpoint::open(&file).await.unwrap();
        checkpoint.advance(9, 3).await.unwrap();
        checkpoint.advance(4, 1).await.unwrap();
        drop(checkpoint);
        let reopened = Checkpoint::open(&file).await.unwrap();
        assert_eq!(reopened.current().await, 9);
        assert_eq!(reopened.writer_epoch().await, 3);
        reopened.reset().await.unwrap();
        assert_eq!(reopened.current().await, 0);
        let _ = tokio::fs::remove_file(file).await;
    }

    #[tokio::test]
    async fn rejects_corrupt_checkpoint() {
        let file = path("checkpoint-corrupt");
        tokio::fs::write(&file, b"WCP1-short").await.unwrap();
        assert!(Checkpoint::open(&file).await.is_err());
        let _ = tokio::fs::remove_file(file).await;
    }
}
