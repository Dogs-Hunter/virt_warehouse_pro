use std::{io::ErrorKind, path::{Path, PathBuf}, time::{SystemTime, UNIX_EPOCH}};

use anyhow::{Context, Result};
use bytes::{BufMut, BytesMut};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
    sync::{mpsc, oneshot},
    time,
};

use crate::{binary, model::Operation};

const HEADER_SIZE: usize = 8;

enum WriteCommand {
    Append { payloads: Vec<Vec<u8>>, completed: oneshot::Sender<Result<()>> },
    Reset { completed: oneshot::Sender<Result<()>> },
}

#[derive(Clone)]
pub struct Wal {
    sender: mpsc::Sender<WriteCommand>,
}

impl Wal {
    pub async fn open(
        path: &Path,
        max_batch: usize,
        flush_interval: std::time::Duration,
    ) -> Result<(Self, Vec<Operation>)> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let (recovered, valid_len) = recover(path).await?;
        let file = OpenOptions::new().create(true).append(true).open(path).await?;
        file.set_len(valid_len).await?;
        let (sender, receiver) = mpsc::channel(max_batch.saturating_mul(4));
        tokio::spawn(writer(file, receiver, max_batch, flush_interval));
        Ok((Self { sender }, recovered))
    }

    pub async fn quarantine(path: &Path) -> Result<PathBuf> {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let quarantined = path.with_extension(format!("wal.corrupt-{timestamp}"));
        tokio::fs::rename(path, &quarantined).await
            .with_context(|| format!("cannot quarantine damaged WAL as {}", quarantined.display()))?;
        Ok(quarantined)
    }

    pub async fn append_many(&self, operations: &[Operation]) -> Result<()> {
        if operations.is_empty() {
            return Ok(());
        }
        let payloads = operations.iter().map(binary::encode_operation).collect::<Result<Vec<_>>>()?;
        let (completed, response) = oneshot::channel();
        self.sender
            .send(WriteCommand::Append {
                payloads,
                completed,
            })
            .await
            .context("WAL writer stopped")?;
        response.await.context("WAL writer dropped acknowledgement")?
    }

    pub async fn reset(&self) -> Result<()> {
        let (completed, response) = oneshot::channel();
        self.sender.send(WriteCommand::Reset { completed }).await.context("WAL writer stopped")?;
        response.await.context("WAL writer dropped reset acknowledgement")?
    }
}

async fn writer(
    mut file: File,
    mut receiver: mpsc::Receiver<WriteCommand>,
    max_batch: usize,
    flush_interval: std::time::Duration,
) {
    while let Some(first) = receiver.recv().await {
        let WriteCommand::Append { payloads, completed } = first else {
            if let WriteCommand::Reset { completed } = first {
                let result = async { file.flush().await?; file.set_len(0).await?; file.sync_all().await }.await.map_err(anyhow::Error::from);
                let _ = completed.send(result);
            }
            continue;
        };
        let mut batch = Vec::with_capacity(max_batch);
        let mut records = payloads.len();
        batch.push((payloads, completed));
        let deadline = time::sleep(flush_interval);
        tokio::pin!(deadline);
        while records < max_batch {
            tokio::select! {
                biased;
                Some(WriteCommand::Append { payloads, completed }) = receiver.recv() => {
                    records += payloads.len();
                    batch.push((payloads, completed));
                },
                () = &mut deadline => break,
            }
        }

        let mut buffer = BytesMut::new();
        for (payloads, _) in &batch {
            for payload in payloads {
                buffer.put_u32_le(payload.len() as u32);
                buffer.put_u32_le(crc32fast::hash(payload));
                buffer.extend_from_slice(payload);
            }
        }
        let result: std::io::Result<()> = async {
            file.write_all(&buffer).await?;
            file.sync_data().await?;
            Ok(())
        }
        .await;

        match result {
            Ok(()) => {
                for (_, completed) in batch {
                    let _ = completed.send(Ok(()));
                }
            }
            Err(error) => {
                let message = error.to_string();
                for (_, completed) in batch {
                    let _ = completed.send(Err(anyhow::anyhow!(message.clone())));
                }
            }
        }
    }
}

async fn recover(path: &Path) -> Result<(Vec<Operation>, u64)> {
    let data = match tokio::fs::read(path).await {
        Ok(data) => data,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok((Vec::new(), 0)),
        Err(error) => return Err(error.into()),
    };
    let mut operations = Vec::new();
    let mut cursor = 0_usize;
    while data.len().saturating_sub(cursor) >= HEADER_SIZE {
        let header = &data[cursor..cursor + HEADER_SIZE];
        let size = u32::from_le_bytes(header[0..4].try_into().expect("four-byte length")) as usize;
        let checksum = u32::from_le_bytes(header[4..8].try_into().expect("four-byte checksum"));
        anyhow::ensure!(size <= 1024 * 1024, "WAL record exceeds 1 MiB");
        let record_end = cursor + HEADER_SIZE + size;
        if record_end > data.len() {
            break;
        }
        let payload = &data[cursor + HEADER_SIZE..record_end];
        anyhow::ensure!(crc32fast::hash(payload) == checksum, "WAL checksum mismatch");
        operations.push(binary::decode_operation(payload)
            .or_else(|_| serde_json::from_slice(payload).context("invalid legacy JSON WAL record"))?);
        cursor = record_end;
    }
    Ok((operations, cursor as u64))
}
