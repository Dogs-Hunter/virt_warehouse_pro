use std::{sync::Arc, time::{Duration, Instant}};

use anyhow::{Context, Result};
use async_nats::jetstream::{self, stream::{DiscardPolicy, StorageType}};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{sync::RwLock, time::sleep};

use crate::{binary::{self, BinaryLogRecord}, model::Operation};

const STREAM: &str = "WAREHOUSE_OPERATIONS";
const SUBJECT: &str = "warehouse.operations";
const WRITER_BUCKET: &str = "WAREHOUSE_WRITER_FENCE";
const WRITER_KEY: &str = "active";
const DLQ_STREAM: &str = "WAREHOUSE_DLQ";
const DLQ_SUBJECT: &str = "warehouse.operations.dlq";

#[derive(Serialize)]
struct DlqRecord {
    source_stream: &'static str,
    source_sequence: u64,
    reason: String,
    payload: Vec<u8>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "record_type", rename_all = "snake_case")]
pub enum LogRecord {
    Fence { epoch: u64, owner: String },
    Operations { epoch: u64, operations: Vec<Operation> },
}

#[derive(Deserialize, Serialize)]
struct LeaseValue { instance_id: String, epoch: u64 }

#[derive(Clone)]
pub struct ReplicatedLog {
    context: jetstream::Context,
}

impl ReplicatedLog {
    pub async fn connect(url: &str) -> Result<Self> {
        let mut last_error = None;
        for attempt in 1..=120 {
            match Self::try_connect(url).await {
                Ok(log) => return Ok(log),
                Err(error) => {
                    last_error = Some(error);
                    if attempt % 10 == 0 {
                        tracing::warn!(attempt, "waiting for three-node replicated log");
                    }
                    sleep(Duration::from_millis(500)).await;
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("replicated log unavailable")))
    }

    async fn try_connect(url: &str) -> Result<Self> {
        let user = std::env::var("WAREHOUSE_NATS_USER").context("WAREHOUSE_NATS_USER is required")?;
        let password = std::env::var("WAREHOUSE_NATS_PASSWORD").context("WAREHOUSE_NATS_PASSWORD is required")?;
        let ca = std::env::var("WAREHOUSE_NATS_CA").context("WAREHOUSE_NATS_CA is required")?;
        let client = async_nats::ConnectOptions::with_user_and_password(user, password)
            .add_root_certificates(std::path::PathBuf::from(ca))
            .require_tls(true)
            .connect(url)
            .await
            .with_context(|| format!("cannot connect to NATS at {url}"))?;
        let context = jetstream::new(client);
        let max_bytes = std::env::var("WAREHOUSE_STREAM_MAX_BYTES").ok()
            .map(|value| value.parse::<i64>()).transpose().context("invalid WAREHOUSE_STREAM_MAX_BYTES")?
            .unwrap_or(16 * 1024 * 1024 * 1024);
        anyhow::ensure!(max_bytes >= 1024 * 1024, "WAREHOUSE_STREAM_MAX_BYTES must be at least 1 MiB");
        let stream_config = jetstream::stream::Config {
                name: STREAM.to_owned(),
                subjects: vec![SUBJECT.to_owned()],
                storage: StorageType::File,
                num_replicas: 3,
                max_bytes,
                discard: DiscardPolicy::New,
                ..Default::default()
            };
        let mut stream = context
            .get_or_create_stream(stream_config.clone())
            .await
            .context("cannot create three-replica operations stream")?;
        let mut info = stream.info().await.context("cannot read stream metadata")?;
        if info.config.max_bytes != max_bytes || info.config.discard != DiscardPolicy::New {
            context.update_stream(stream_config).await.context("cannot apply safe operations-stream retention")?;
            stream = context.get_stream(STREAM).await?;
            info = stream.info().await.context("cannot verify operations-stream retention")?;
        }
        anyhow::ensure!(
            info.config.num_replicas == 3,
            "stream has {} replicas instead of 3",
            info.config.num_replicas
        );
        anyhow::ensure!(info.config.max_bytes == max_bytes && info.config.discard == DiscardPolicy::New,
            "operations stream retention policy was not applied");
        context.get_or_create_stream(jetstream::stream::Config {
            name: DLQ_STREAM.to_owned(),
            subjects: vec![DLQ_SUBJECT.to_owned()],
            storage: StorageType::File,
            num_replicas: 3,
            ..Default::default()
        }).await.context("cannot create three-replica DLQ stream")?;
        tracing::info!(stream = STREAM, replicas = 3, "replicated log ready");
        Ok(Self { context })
    }

    pub async fn append(&self, epoch: u64, operations: &[Operation]) -> Result<u64> {
        if operations.is_empty() {
            return Ok(0);
        }
        let payload = binary::encode_log_operations(epoch, operations)?;
        // A publish acknowledgement may be lost after the quorum has already
        // committed the message. Reusing the payload digest lets JetStream
        // collapse an immediate producer retry while application deduplication
        // still protects retries outside the server duplicate window.
        let message_id = format!("operations-{:x}", Sha256::digest(&payload));
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", message_id);
        let acknowledgement = self
            .context
            .publish_with_headers(SUBJECT, headers, payload.into())
            .await
            .context("replicated publish failed")?
            .await
            .context("replicated quorum acknowledgement failed")?;
        Ok(acknowledgement.sequence)
    }

    async fn append_fence(&self, epoch: u64, owner: &str) -> Result<u64> {
        let payload = binary::encode_log_fence(epoch, owner)?;
        let acknowledgement = self.context.publish(SUBJECT, payload.into()).await?.await?;
        Ok(acknowledgement.sequence)
    }

    pub async fn last_sequence(&self) -> Result<u64> {
        let stream = self.context.get_stream(STREAM).await?;
        Ok(stream.get_info().await?.state.last_sequence)
    }

    pub async fn storage_state(&self) -> Result<(u64, u64, u64, u64)> {
        let mut stream = self.context.get_stream(STREAM).await?;
        let info = stream.info().await?;
        Ok((info.state.last_sequence, info.state.messages, info.state.bytes, info.config.max_bytes.max(0) as u64))
    }

    pub async fn read(&self, sequence: u64) -> Result<LogRecord> {
        let stream = self.context.get_stream(STREAM).await?;
        let message = stream.get_raw_message(sequence).await?;
        let decoded = binary::decode_log(&message.payload).map(|record| match record {
                BinaryLogRecord::Fence { epoch, owner } => LogRecord::Fence { epoch, owner },
                BinaryLogRecord::Operations { epoch, operations } => LogRecord::Operations { epoch, operations },
            })
            .or_else(|_| serde_json::from_slice::<LogRecord>(&message.payload)
            .or_else(|_| serde_json::from_slice::<Vec<Operation>>(&message.payload)
                .map(|operations| LogRecord::Operations { epoch: 0, operations })))
            .context("invalid replicated log record")
            .and_then(|record| {
                let operations = match &record { LogRecord::Operations { operations, .. } => operations, LogRecord::Fence { .. } => return Ok(record) };
                for operation in operations {
                    operation.validate().map_err(|reason| anyhow::anyhow!(reason))?;
                }
                Ok(record)
            });
        match decoded {
            Ok(operations) => Ok(operations),
            Err(error) => {
                let record = DlqRecord {
                    source_stream: STREAM,
                    source_sequence: sequence,
                    reason: error.to_string(),
                    payload: message.payload.to_vec(),
                };
                let payload = serde_json::to_vec(&record)?;
                let mut headers = async_nats::HeaderMap::new();
                headers.insert("Nats-Msg-Id", format!("{STREAM}-{sequence}"));
                self.context.publish_with_headers(DLQ_SUBJECT, headers, payload.into()).await?
                    .await.context("DLQ quorum acknowledgement failed")?;
                tracing::error!(sequence, %error, "poison replicated event moved to DLQ");
                Ok(LogRecord::Operations { epoch: u64::MAX, operations: Vec::new() })
            }
        }
    }

    pub async fn quarantine_record(&self, sequence: u64, reason: String) -> Result<()> {
        let stream = self.context.get_stream(STREAM).await?;
        let message = stream.get_raw_message(sequence).await?;
        let record = DlqRecord { source_stream: STREAM, source_sequence: sequence, reason, payload: message.payload.to_vec() };
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", format!("{STREAM}-{sequence}"));
        self.context.publish_with_headers(DLQ_SUBJECT, headers, serde_json::to_vec(&record)?.into()).await?.await?;
        Ok(())
    }

    pub async fn start_writer_lease(&self, instance_id: String, initial_delay: Duration) -> Result<WriterLease> {
        sleep(initial_delay).await;
        let mut store = None;
        let mut last_error = None;
        for _ in 0..20 {
            match self.context.create_or_update_key_value(jetstream::kv::Config {
                bucket: WRITER_BUCKET.to_owned(),
                max_age: Duration::from_secs(5),
                storage: StorageType::File,
                num_replicas: 3,
                ..Default::default()
            }).await {
                Ok(created) => { store = Some(created); break; }
                Err(error) => {
                    last_error = Some(error);
                    if let Ok(existing) = self.context.get_key_value(WRITER_BUCKET).await {
                        store = Some(existing);
                        break;
                    }
                    sleep(Duration::from_millis(250)).await;
                }
            }
        }
        let store = store.ok_or_else(|| anyhow::anyhow!(
            "cannot initialize writer fencing bucket: {:?}", last_error
        ))?;
        let state = Arc::new(RwLock::new(LeaseState { owned: false, valid_until: Instant::now(), epoch: 0 }));
        let lease = WriterLease { state: state.clone() };
        let log = self.clone();
        tokio::spawn(async move {
            loop {
                let (renewed, definitely_lost) = match store.entry(WRITER_KEY).await {
                    Ok(Some(entry)) => match serde_json::from_slice::<LeaseValue>(&entry.value) {
                        Ok(value) if value.instance_id == instance_id && value.epoch > 0 => {
                            let encoded = serde_json::to_vec(&value).unwrap();
                            (store.update(WRITER_KEY, encoded.into(), entry.revision).await.ok().map(|_| value.epoch), false)
                        }
                        _ => (None, true),
                    },
                    Ok(None) => {
                        let provisional = serde_json::to_vec(&LeaseValue { instance_id: instance_id.clone(), epoch: 0 }).unwrap();
                        match store.create(WRITER_KEY, provisional.into()).await {
                            Ok(revision) => {
                                let value = LeaseValue { instance_id: instance_id.clone(), epoch: revision };
                                let encoded = serde_json::to_vec(&value).unwrap();
                                match store.update(WRITER_KEY, encoded.into(), revision).await {
                                    Ok(_) if log.append_fence(revision, &instance_id).await.is_ok() => (Some(revision), false),
                                    _ => (None, true),
                                }
                            }
                            Err(_) => (None, true),
                        }
                    }
                    Err(_) => (None, false),
                };
                let mut current = state.write().await;
                if let Some(epoch) = renewed {
                    current.owned = true;
                    current.epoch = epoch;
                    current.valid_until = Instant::now() + Duration::from_secs(4);
                } else if definitely_lost || Instant::now() >= current.valid_until {
                    current.owned = false;
                }
                drop(current);
                sleep(Duration::from_millis(750)).await;
            }
        });
        Ok(lease)
    }
}

struct LeaseState { owned: bool, valid_until: Instant, epoch: u64 }

#[derive(Clone)]
pub struct WriterLease { state: Arc<RwLock<LeaseState>> }

impl WriterLease {
    pub async fn owns_valid_lease_for(&self, required: Duration) -> bool {
        let state = self.state.read().await;
        state.owned && Instant::now() + required < state.valid_until
    }
    pub async fn valid_epoch_for(&self, required: Duration) -> Option<u64> {
        let state = self.state.read().await;
        (state.owned && Instant::now() + required < state.valid_until).then_some(state.epoch)
    }
}
