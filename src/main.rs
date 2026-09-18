mod cluster;
mod binary;
mod checkpoint;
mod config;
mod dedup_disk;
mod model;
mod snapshot;
mod state;
mod wal;

use std::{path::PathBuf, sync::{Arc, atomic::{AtomicU64, Ordering}}, time::{Duration, Instant}};

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{header, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use config::Config;
use cluster::{LogRecord, ReplicatedLog, WriterLease};
use checkpoint::Checkpoint;
use dedup_disk::DiskDedup;
use futures::{stream, StreamExt};
use model::{ApplyResult, Operation};
use serde::Serialize;
use state::Store;
use wal::Wal;

#[derive(Clone)]
struct AppState {
    store: Store,
    started: Arc<Instant>,
    replicated_log: Option<ReplicatedLog>,
    fail_after_replicate_id: Option<Arc<str>>,
    writer_lease: Option<WriterLease>,
    checkpoint: Checkpoint,
    wal_path: Arc<PathBuf>,
    metrics: Arc<Metrics>,
    history_checkpoint: Arc<AtomicU64>,
}

#[derive(Default)]
struct Metrics {
    applied: AtomicU64,
    duplicates: AtomicU64,
    conflicts: AtomicU64,
    unavailable: AtomicU64,
    batch_requests: AtomicU64,
    pipeline_operations: AtomicU64,
    json_decode_ns: AtomicU64,
    preflight_ns: AtomicU64,
    quorum_ns: AtomicU64,
    apply_ns: AtomicU64,
    finish_ns: AtomicU64,
    handler_ns: AtomicU64,
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
    uptime_seconds: u64,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

#[derive(Serialize)]
struct BalanceBody {
    owner_id: String,
    sku: String,
    balance: i64,
    generation: u64,
}

#[derive(Serialize)]
struct OwnerBalance { sku: String, balance: i64 }

#[derive(Serialize)]
struct OwnerSummary { owner_id: String, generation: u64, total_balance: i64, positions: Vec<OwnerBalance> }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warehouse_lab=info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
    let config = Config::from_env()?;
    let wal_path = config.data_dir.join("operations.wal");
    let checkpoint_path = config.data_dir.join("replicated.checkpoint");
    let snapshot_path = config.data_dir.join("state.snapshot");
    let mut rebuild_from_quorum = false;
    let mut snapshot = match snapshot::load(&snapshot_path).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            let quarantined = snapshot::quarantine(&snapshot_path).await?;
            tracing::error!(%error, path = %quarantined.display(), "damaged snapshot quarantined");
            rebuild_from_quorum = config.nats_url.is_some();
            None
        }
    };
    let dedup_path = config.data_dir.join("dedup.redb");
    let history_path = config.data_dir.join("history.redb");
    if dedup_path.exists() && !history_path.exists() && config.nats_url.is_some() {
        let timestamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis();
        let archived = config.data_dir.join(format!("dedup.redb.monolithic-{timestamp}"));
        std::fs::rename(&dedup_path, &archived)?;
        tracing::warn!(path = %archived.display(), "legacy monolithic read index archived; rebuilding split indexes from quorum");
        rebuild_from_quorum = true;
        snapshot = None;
    }
    let mut dedup_existed = dedup_path.exists();
    if rebuild_from_quorum && snapshot.is_none() && dedup_existed {
        let quarantined = dedup_disk::quarantine(&dedup_path)?;
        tracing::warn!(path = %quarantined.display(), "discarding disk dedup index because its materialized snapshot is unavailable");
        dedup_existed = false;
    }
    let disk_dedup = match DiskDedup::open(&dedup_path) {
        Ok(index) => index,
        Err(error) if config.nats_url.is_some() => {
            let quarantined = dedup_disk::quarantine(&dedup_path)?;
            tracing::error!(%error, path = %quarantined.display(), "damaged disk dedup index quarantined; rebuilding from quorum");
            rebuild_from_quorum = true;
            snapshot = None;
            DiskDedup::open(&dedup_path)?
        }
        Err(error) => return Err(error),
    };
    if !dedup_existed && snapshot.as_ref().is_some_and(|value| value.version >= 2) {
        tracing::warn!("disk dedup index is missing; rebuilding materialized state from quorum");
        rebuild_from_quorum = config.nats_url.is_some();
        snapshot = None;
    }
    let snapshot_sequence = snapshot.as_ref().map_or(0, |value| value.sequence);
    let snapshot_epoch = snapshot.as_ref().map_or(0, |value| value.writer_epoch);
    let (wal, recovered) = match Wal::open(
        &wal_path,
        config.wal_batch,
        config.wal_flush_interval,
    )
    .await {
        Ok(opened) => opened,
        Err(error) if config.nats_url.is_some() => {
            let quarantined = Wal::quarantine(&wal_path).await?;
            tracing::error!(%error, path = %quarantined.display(), "damaged local WAL quarantined; rebuilding from quorum");
            rebuild_from_quorum = true;
            Wal::open(&wal_path, config.wal_batch, config.wal_flush_interval).await?
        }
        Err(error) => return Err(error),
    };
    tracing::info!(records = recovered.len(), "WAL recovery completed");
    let store = Store::start(config.shards, wal.clone(), recovered, snapshot, disk_dedup)?;
    let replicated_log = match config.nats_url.as_deref() {
        Some(url) => Some(ReplicatedLog::connect(url).await?),
        None => {
            tracing::warn!("running without replicated log; single-node mode only");
            None
        }
    };
    let checkpoint = Checkpoint::open(&checkpoint_path).await?;
    if rebuild_from_quorum {
        checkpoint.reset().await?;
    } else if snapshot_sequence > checkpoint.current().await {
        checkpoint.advance(snapshot_sequence, snapshot_epoch).await?;
    }
    if let Some(log) = &replicated_log {
        catch_up(&store, log, &checkpoint).await?;
    }
    let compact_sequence = checkpoint.current().await;
    let compacted = store.snapshot(compact_sequence, checkpoint.writer_epoch().await).await?;
    store.sync_disk_dedup()?;
    snapshot::save(&snapshot_path, &compacted).await?;
    wal.reset().await?;
    tracing::info!(sequence = compact_sequence, balances = compacted.balances.len(), dedup = store.disk_dedup_len()?, "state snapshot committed and WAL compacted");
    if config.live_tail {
        if let Some(log) = replicated_log.clone() {
            tokio::spawn(run_live_tail(store.clone(), log, checkpoint.clone()));
        }
    }
    let (history_sequence, _) = store.prepare_binary_history_migration()?;
    let history_checkpoint = Arc::new(AtomicU64::new(history_sequence));
    if let Some(log) = replicated_log.clone() {
        tokio::spawn(run_history_tail(store.clone(), log, history_checkpoint.clone()));
    }
    let writer_lease = match &replicated_log {
        Some(log) => Some(log.start_writer_lease(config.instance_id.clone(), config.writer_priority_delay).await?),
        None => None,
    };
    let state = AppState {
        store,
        started: Arc::new(Instant::now()),
        replicated_log,
        fail_after_replicate_id: config.fail_after_replicate_id.map(Arc::from),
        writer_lease,
        checkpoint,
        wal_path: Arc::new(wal_path),
        metrics: Arc::new(Metrics::default()),
        history_checkpoint,
    };
    let app = Router::new()
        .route("/live", get(live))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/v1/operations", post(apply_one))
        .route("/v1/operations/:operation_id", get(get_operation))
        .route("/v1/operations/read-batch", post(get_operations_batch))
        .route("/v1/operations/by-owner/:owner_id", get(get_operations_by_owner))
        .route("/v1/operations/by-sku/:sku", get(get_operations_by_sku))
        .route("/v1/operations/batch", post(apply_batch))
        .route("/v1/balances/:owner_id/:sku", get(get_balance))
        .route("/v1/balances/:owner_id", get(get_owner_balances))
        .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    tracing::info!(address = %config.bind, "warehouse lab started");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let checkpoint = state.checkpoint.current().await;
    let (quorum, last_sequence, stream_messages, stream_bytes, stream_max_bytes) = match &state.replicated_log {
        Some(log) => match tokio::time::timeout(Duration::from_millis(500), log.storage_state()).await {
            Ok(Ok((sequence, messages, bytes, max_bytes))) => (1_u64, sequence, messages, bytes, max_bytes),
            _ => (0, checkpoint, 0, 0, 0),
        },
        None => (0, checkpoint, 0, 0, 0),
    };
    let lease = match &state.writer_lease {
        Some(lease) if lease.owns_valid_lease_for(Duration::from_millis(250)).await => 1,
        _ => 0,
    };
    let writer_epoch = match &state.writer_lease {
        Some(lease) => lease.valid_epoch_for(Duration::from_millis(250)).await.unwrap_or(0),
        None => 0,
    };
    let wal_bytes = tokio::fs::metadata(state.wal_path.as_ref()).await.map_or(0, |value| value.len());
    let dedup_path = state.wal_path.with_file_name("dedup.redb");
    let dedup_bytes = tokio::fs::metadata(&dedup_path).await.map_or(0, |value| value.len());
    let history_bytes = tokio::fs::metadata(state.wal_path.with_file_name("history.redb")).await.map_or(0, |value| value.len());
    let dedup_entries = state.store.disk_dedup_len().unwrap_or(0);
    let history_entries = state.store.history_len().unwrap_or(0);
    let history_checkpoint = state.history_checkpoint.load(Ordering::Acquire);
    let history_lag = last_sequence.saturating_sub(history_checkpoint);
    let memory_bytes = process_memory_bytes().await.unwrap_or(0);
    let lag = last_sequence.saturating_sub(checkpoint);
    let body = format!(concat!(
        "# TYPE warehouse_operations_applied_total counter\nwarehouse_operations_applied_total {}\n",
        "# TYPE warehouse_operations_duplicate_total counter\nwarehouse_operations_duplicate_total {}\n",
        "# TYPE warehouse_conflicts_total counter\nwarehouse_conflicts_total {}\n",
        "# TYPE warehouse_unavailable_total counter\nwarehouse_unavailable_total {}\n",
        "# TYPE warehouse_quorum_available gauge\nwarehouse_quorum_available {}\n",
        "# TYPE warehouse_writer_lease_owned gauge\nwarehouse_writer_lease_owned {}\n",
        "# TYPE warehouse_writer_epoch gauge\nwarehouse_writer_epoch {}\n",
        "# TYPE warehouse_replication_checkpoint gauge\nwarehouse_replication_checkpoint {}\n",
        "# TYPE warehouse_replication_last_sequence gauge\nwarehouse_replication_last_sequence {}\n",
        "# TYPE warehouse_replication_lag gauge\nwarehouse_replication_lag {}\n",
        "# TYPE warehouse_wal_bytes gauge\nwarehouse_wal_bytes {}\n",
        "# TYPE warehouse_process_memory_bytes gauge\nwarehouse_process_memory_bytes {}\n",
        "# TYPE warehouse_disk_dedup_entries gauge\nwarehouse_disk_dedup_entries {}\n",
        "# TYPE warehouse_disk_dedup_bytes gauge\nwarehouse_disk_dedup_bytes {}\n"
        ,"# TYPE warehouse_history_entries gauge\nwarehouse_history_entries {}\n"
        ,"# TYPE warehouse_history_bytes gauge\nwarehouse_history_bytes {}\n"
        ,"# TYPE warehouse_history_checkpoint gauge\nwarehouse_history_checkpoint {}\n"
        ,"# TYPE warehouse_history_lag gauge\nwarehouse_history_lag {}\n"
        ,"# TYPE warehouse_stream_messages gauge\nwarehouse_stream_messages {}\n"
        ,"# TYPE warehouse_stream_bytes gauge\nwarehouse_stream_bytes {}\n"
        ,"# TYPE warehouse_stream_max_bytes gauge\nwarehouse_stream_max_bytes {}\n"
        ,"# TYPE warehouse_pipeline_batch_requests_total counter\nwarehouse_pipeline_batch_requests_total {}\n"
        ,"# TYPE warehouse_pipeline_operations_total counter\nwarehouse_pipeline_operations_total {}\n"
        ,"# TYPE warehouse_pipeline_json_decode_seconds_total counter\nwarehouse_pipeline_json_decode_seconds_total {:.9}\n"
        ,"# TYPE warehouse_pipeline_preflight_seconds_total counter\nwarehouse_pipeline_preflight_seconds_total {:.9}\n"
        ,"# TYPE warehouse_pipeline_quorum_seconds_total counter\nwarehouse_pipeline_quorum_seconds_total {:.9}\n"
        ,"# TYPE warehouse_pipeline_wal_apply_seconds_total counter\nwarehouse_pipeline_wal_apply_seconds_total {:.9}\n"
        ,"# TYPE warehouse_pipeline_publish_seconds_total counter\nwarehouse_pipeline_publish_seconds_total {:.9}\n"
        ,"# TYPE warehouse_pipeline_handler_seconds_total counter\nwarehouse_pipeline_handler_seconds_total {:.9}\n"
    ), state.metrics.applied.load(Ordering::Relaxed), state.metrics.duplicates.load(Ordering::Relaxed),
       state.metrics.conflicts.load(Ordering::Relaxed), state.metrics.unavailable.load(Ordering::Relaxed),
       quorum, lease, writer_epoch, checkpoint, last_sequence, lag, wal_bytes, memory_bytes, dedup_entries, dedup_bytes, history_entries, history_bytes, history_checkpoint, history_lag,
       stream_messages, stream_bytes, stream_max_bytes,
       state.metrics.batch_requests.load(Ordering::Relaxed), state.metrics.pipeline_operations.load(Ordering::Relaxed),
       seconds(state.metrics.json_decode_ns.load(Ordering::Relaxed)), seconds(state.metrics.preflight_ns.load(Ordering::Relaxed)),
       seconds(state.metrics.quorum_ns.load(Ordering::Relaxed)), seconds(state.metrics.apply_ns.load(Ordering::Relaxed)),
       seconds(state.metrics.finish_ns.load(Ordering::Relaxed)), seconds(state.metrics.handler_ns.load(Ordering::Relaxed)));
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")], body)
}

fn seconds(nanoseconds: u64) -> f64 { nanoseconds as f64 / 1_000_000_000.0 }
fn record_duration(target: &AtomicU64, started: Instant) { target.fetch_add(started.elapsed().as_nanos().min(u64::MAX as u128) as u64, Ordering::Relaxed); }

async fn process_memory_bytes() -> Option<u64> {
    let status = tokio::fs::read_to_string("/proc/self/status").await.ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    line.split_whitespace().nth(1)?.parse::<u64>().ok()?.checked_mul(1024)
}

fn record_results(metrics: &Metrics, results: &[ApplyResult]) {
    for result in results {
        match result.status {
            model::ApplyStatus::Applied => { metrics.applied.fetch_add(1, Ordering::Relaxed); }
            model::ApplyStatus::Duplicate => { metrics.duplicates.fetch_add(1, Ordering::Relaxed); }
        }
    }
}

fn record_error(metrics: &Metrics, error: &(StatusCode, Json<ErrorBody>)) {
    if error.0 == StatusCode::CONFLICT { metrics.conflicts.fetch_add(1, Ordering::Relaxed); }
    if error.0 == StatusCode::SERVICE_UNAVAILABLE { metrics.unavailable.fetch_add(1, Ordering::Relaxed); }
}

async fn run_live_tail(store: Store, log: ReplicatedLog, checkpoint: Checkpoint) {
    loop {
        let current = checkpoint.current().await;
        match log.last_sequence().await {
            Ok(last) if last > current => {
                if let Err(error) = catch_up(&store, &log, &checkpoint).await {
                    tracing::error!(%error, "live replicated-log catch-up failed");
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                }
            }
            Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(25)).await,
            Err(error) => {
                tracing::error!(%error, "cannot read replicated-log tail");
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        }
    }
}

async fn run_history_tail(store: Store, log: ReplicatedLog, published: Arc<AtomicU64>) {
    loop {
        let (current, mut epoch) = match store.history_progress() {
            Ok(value) => value,
            Err(error) => {
                tracing::error!(%error, "cannot read history checkpoint");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        match log.last_sequence().await {
            Ok(last) if last > current => {
                for sequence in current.saturating_add(1)..=last {
                    let record = match log.read(sequence).await {
                        Ok(value) => value,
                        Err(error) => {
                            tracing::error!(sequence, %error, "history backfill read failed");
                            break;
                        }
                    };
                    let operations = match record {
                        LogRecord::Fence { epoch: next, .. } => { if next > epoch { epoch = next; } Vec::new() }
                        LogRecord::Operations { epoch: record_epoch, operations } if record_epoch == epoch => operations,
                        LogRecord::Operations { .. } => Vec::new(),
                    };
                    if let Err(error) = store.materialize_history(sequence, epoch, &operations) {
                        tracing::error!(sequence, %error, "history backfill write failed");
                        break;
                    }
                    published.store(sequence, Ordering::Release);
                }
                if published.load(Ordering::Acquire) == last {
                    if let Err(error) = store.sync_disk_dedup() { tracing::error!(%error, "history backfill sync failed"); }
                    if let Err(error) = store.complete_binary_history_migration() { tracing::error!(%error, "cannot complete binary history migration"); }
                    tracing::info!(last_sequence = last, entries = store.history_len().unwrap_or(0), "history backfill completed");
                }
            }
            Ok(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            Err(error) => {
                tracing::error!(%error, "cannot read history log tail");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

async fn ensure_history_complete(state: &AppState) -> Result<(), (StatusCode, Json<ErrorBody>)> {
    let required = state.checkpoint.current().await;
    let current = state.history_checkpoint.load(Ordering::Acquire);
    if current < required {
        return Err((StatusCode::SERVICE_UNAVAILABLE, Json(ErrorBody {
            error: format!("history index rebuilding: checkpoint {current}, required {required}"),
        })));
    }
    Ok(())
}

async fn catch_up(store: &Store, log: &ReplicatedLog, checkpoint: &Checkpoint) -> anyhow::Result<()> {
    let last_sequence = log.last_sequence().await?;
    let first_sequence = checkpoint.current().await.saturating_add(1);
    let mut operations = 0_u64;
    let mut writer_epoch = checkpoint.writer_epoch().await;
    let mut batches = stream::iter(first_sequence..=last_sequence)
        .map(|sequence| async move { Ok::<_, anyhow::Error>((sequence, log.read(sequence).await?)) })
        .buffered(64);
    let mut pending_checkpoint = None;
    let mut since_checkpoint = 0_usize;
    while let Some(result) = batches.next().await {
        let (sequence, record) = result?;
        match record {
            LogRecord::Fence { epoch, owner } => {
                if epoch > writer_epoch {
                    writer_epoch = epoch;
                    tracing::info!(epoch, %owner, sequence, "writer fence activated");
                }
            }
            LogRecord::Operations { epoch, operations: batch } if epoch == writer_epoch => {
                operations += batch.len() as u64;
                if !batch.is_empty() { store.restore_chunk(batch).await?; }
            }
            LogRecord::Operations { epoch: u64::MAX, .. } => {}
            LogRecord::Operations { epoch, .. } => {
                tracing::error!(sequence, epoch, active_epoch = writer_epoch, "stale or unfenced writer batch rejected");
                log.quarantine_record(sequence, format!("writer epoch {epoch} rejected; active epoch is {writer_epoch}")).await?;
            }
        }
        pending_checkpoint = Some((sequence, writer_epoch));
        since_checkpoint += 1;
        if since_checkpoint >= 64 {
            checkpoint.advance(sequence, writer_epoch).await?;
            since_checkpoint = 0;
            pending_checkpoint = None;
        }
    }
    if let Some((sequence, epoch)) = pending_checkpoint { checkpoint.advance(sequence, epoch).await?; }
    tracing::info!(last_sequence, operations, "replicated log catch-up completed");
    Ok(())
}

async fn get_balance(
    State(state): State<AppState>,
    Path((owner_id, sku)): Path<(String, String)>,
) -> Result<Json<BalanceBody>, (StatusCode, Json<ErrorBody>)> {
    let balance = state
        .store
        .balance(owner_id.clone(), sku.clone())
        .await
        .map_err(internal)?;
    Ok(Json(BalanceBody {
        owner_id,
        sku,
        balance,
        generation: state.checkpoint.current().await,
    }))
}

async fn get_owner_balances(State(state): State<AppState>, Path(owner_id): Path<String>) -> Json<OwnerSummary> {
    let positions = state.store.balances_by_owner(&owner_id).await.into_iter().map(|(sku, balance)| OwnerBalance { sku, balance }).collect::<Vec<_>>();
    let total_balance = positions.iter().map(|value| value.balance).sum();
    Json(OwnerSummary { owner_id, generation: state.checkpoint.current().await, total_balance, positions })
}

async fn get_operation(State(state): State<AppState>, Path(operation_id): Path<String>) -> impl IntoResponse {
    if let Err(error) = ensure_history_complete(&state).await { return error.into_response(); }
    match state.store.operation(&operation_id) {
        Ok(Some(operation)) => Json(operation).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => internal(error).into_response(),
    }
}

async fn get_operations_batch(State(state): State<AppState>, body: Bytes) -> Result<Json<Vec<Operation>>, (StatusCode, Json<ErrorBody>)> {
    ensure_history_complete(&state).await?;
    let ids: Vec<String> = serde_json::from_slice(&body).map_err(|error| bad_request(format!("invalid JSON ID batch: {error}")))?;
    if ids.is_empty() || ids.len() > 10_000 { return Err(bad_request("read batch must contain 1..=10000 IDs")); }
    let operations = state.store.operations(&ids).map_err(internal)?;
    if operations.len() != ids.len() { return Err((StatusCode::NOT_FOUND, Json(ErrorBody { error: "one or more operations were not found".into() }))); }
    Ok(Json(operations))
}

#[derive(serde::Deserialize)]
struct HistoryQuery { cursor: Option<String>, limit: Option<usize> }

#[derive(Serialize)]
struct HistoryPage { operations: Vec<Operation>, next_cursor: Option<String> }

fn history_page(operations: Vec<Operation>, limit: usize) -> HistoryPage {
    let next_cursor = (operations.len() == limit).then(|| operations.last().unwrap().operation_id.clone());
    HistoryPage { operations, next_cursor }
}

async fn get_operations_by_owner(State(state): State<AppState>, Path(owner): Path<String>, Query(query): Query<HistoryQuery>) -> Result<Json<HistoryPage>, (StatusCode, Json<ErrorBody>)> {
    ensure_history_complete(&state).await?;
    let limit = query.limit.unwrap_or(100);
    if !(1..=1000).contains(&limit) { return Err(bad_request("limit must be 1..=1000")); }
    let operations = state.store.operations_by_owner(&owner, query.cursor.as_deref(), limit).map_err(internal)?;
    Ok(Json(history_page(operations, limit)))
}

async fn get_operations_by_sku(State(state): State<AppState>, Path(sku): Path<String>, Query(query): Query<HistoryQuery>) -> Result<Json<HistoryPage>, (StatusCode, Json<ErrorBody>)> {
    ensure_history_complete(&state).await?;
    let limit = query.limit.unwrap_or(100);
    if !(1..=1000).contains(&limit) { return Err(bad_request("limit must be 1..=1000")); }
    let operations = state.store.operations_by_sku(&sku, query.cursor.as_deref(), limit).map_err(internal)?;
    Ok(Json(history_page(operations, limit)))
}

async fn live(State(state): State<AppState>) -> Json<Health> {
    Json(Health {
        status: "ok",
        uptime_seconds: state.started.elapsed().as_secs(),
    })
}

async fn ready(
    State(state): State<AppState>,
) -> Result<Json<Health>, (StatusCode, Json<ErrorBody>)> {
    if let Some(log) = &state.replicated_log {
        match tokio::time::timeout(Duration::from_millis(750), log.last_sequence()).await {
            Ok(Ok(_)) => {}
            _ => return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorBody { error: "replicated quorum unavailable".into() }),
            )),
        }
    }
    if let Some(lease) = &state.writer_lease {
        if !lease.owns_valid_lease_for(Duration::from_millis(250)).await {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorBody { error: "node does not own the writer lease".into() }),
            ));
        }
    }
    Ok(Json(Health {
        status: "ready",
        uptime_seconds: state.started.elapsed().as_secs(),
    }))
}

async fn apply_one(
    State(state): State<AppState>,
    Json(operation): Json<Operation>,
) -> Result<Json<ApplyResult>, (StatusCode, Json<ErrorBody>)> {
    let epoch = match ensure_writer(&state).await { Ok(epoch) => epoch, Err(error) => { record_error(&state.metrics, &error); return Err(error); } };
    validate(&operation)?;
    let operations = vec![operation];
    let new_operations = match state.store.preflight_batch(&operations).await.map_err(internal) {
        Ok(value) => value,
        Err(error) => { record_error(&state.metrics, &error); return Err(error); }
    };
    let is_new = new_operations[0];
    if is_new {
        if let Err(error) = replicate(&state, epoch, &operations).await {
            state.store.finish_batch(&operations, &new_operations, false).await;
            record_error(&state.metrics, &error); return Err(error);
        }
        if ensure_writer(&state).await.ok() != Some(epoch) {
            let error = internal(anyhow::anyhow!("writer epoch changed during commit"));
            state.store.finish_batch(&operations, &new_operations, false).await;
            record_error(&state.metrics, &error); return Err(error);
        }
        trigger_post_quorum_failpoint(&state, &operations[0].operation_id);
    }
    match state.store.apply_preflight_batch(operations.clone(), &new_operations).await {
        Ok(mut results) => {
            state.store.finish_batch(&operations, &new_operations, true).await;
            record_results(&state.metrics, &results);
            Ok(Json(results.remove(0)))
        }
        Err(error) => {
            state.store.finish_batch(&operations, &new_operations, false).await;
            let error = internal(error); record_error(&state.metrics, &error); Err(error)
        }
    }
}

fn trigger_post_quorum_failpoint(state: &AppState, operation_id: &str) {
    if state.fail_after_replicate_id.as_deref() == Some(operation_id) {
        tracing::error!(operation_id, "triggering configured crash after quorum commit");
        std::process::abort();
    }
}

async fn apply_batch(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<Json<Vec<ApplyResult>>, (StatusCode, Json<ErrorBody>)> {
    let handler_started = Instant::now();
    let decode_started = Instant::now();
    let operations: Vec<Operation> = serde_json::from_slice(&body).map_err(|error| bad_request(format!("invalid JSON batch: {error}")))?;
    record_duration(&state.metrics.json_decode_ns, decode_started);
    state.metrics.batch_requests.fetch_add(1, Ordering::Relaxed);
    state.metrics.pipeline_operations.fetch_add(operations.len() as u64, Ordering::Relaxed);
    let epoch = match ensure_writer(&state).await { Ok(epoch) => epoch, Err(error) => { record_error(&state.metrics, &error); return Err(error); } };
    if operations.is_empty() || operations.len() > 10_000 {
        return Err(bad_request("batch must contain 1..=10000 operations"));
    }
    for operation in &operations {
        validate(operation)?;
    }
    let preflight_started = Instant::now();
    let new_operations = match state.store.preflight_batch(&operations).await.map_err(internal) {
        Ok(value) => value,
        Err(error) => { record_error(&state.metrics, &error); return Err(error); }
    };
    record_duration(&state.metrics.preflight_ns, preflight_started);
    let durable = operations.iter().zip(&new_operations)
        .filter_map(|(operation, is_new)| (*is_new).then_some(operation.clone()))
        .collect::<Vec<_>>();
    if !durable.is_empty() {
        let quorum_started = Instant::now();
        if let Err(error) = replicate(&state, epoch, &durable).await {
            state.store.finish_batch(&operations, &new_operations, false).await;
            record_error(&state.metrics, &error); return Err(error);
        }
        record_duration(&state.metrics.quorum_ns, quorum_started);
        if ensure_writer(&state).await.ok() != Some(epoch) {
            let error = internal(anyhow::anyhow!("writer epoch changed during commit"));
            state.store.finish_batch(&operations, &new_operations, false).await;
            record_error(&state.metrics, &error); return Err(error);
        }
    }
    let apply_started = Instant::now();
    match state.store.apply_preflight_batch(operations.clone(), &new_operations).await {
        Ok(results) => {
            record_duration(&state.metrics.apply_ns, apply_started);
            let finish_started = Instant::now();
            state.store.finish_batch(&operations, &new_operations, true).await;
            record_duration(&state.metrics.finish_ns, finish_started);
            record_results(&state.metrics, &results);
            record_duration(&state.metrics.handler_ns, handler_started);
            Ok(Json(results))
        }
        Err(error) => {
            state.store.finish_batch(&operations, &new_operations, false).await;
            let error = internal(error); record_error(&state.metrics, &error); Err(error)
        }
    }
}

async fn ensure_writer(state: &AppState) -> Result<u64, (StatusCode, Json<ErrorBody>)> {
    if let Some(lease) = &state.writer_lease {
        if let Some(epoch) = lease.valid_epoch_for(Duration::from_millis(2500)).await { return Ok(epoch); } else {
            return Err((StatusCode::SERVICE_UNAVAILABLE, Json(ErrorBody {
                error: "node does not own the writer lease".into(),
            })));
        }
    }
    Ok(0)
}

async fn replicate(
    state: &AppState,
    epoch: u64,
    operations: &[Operation],
) -> Result<Option<u64>, (StatusCode, Json<ErrorBody>)> {
    if let Some(log) = &state.replicated_log {
        match tokio::time::timeout(Duration::from_secs(2), log.append(epoch, operations)).await {
            Ok(result) => result.map(Some).map_err(internal),
            Err(_) => Err(internal(anyhow::anyhow!("replicated quorum acknowledgement timed out"))),
        }
    } else {
        Ok(None)
    }
}

fn validate(operation: &Operation) -> Result<(), (StatusCode, Json<ErrorBody>)> {
    operation.validate().map_err(bad_request)
}

fn bad_request(message: impl ToString) -> (StatusCode, Json<ErrorBody>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorBody {
            error: message.to_string(),
        }),
    )
}

fn internal(error: anyhow::Error) -> (StatusCode, Json<ErrorBody>) {
    if let Some(conflict) = error.downcast_ref::<state::ConflictError>() {
        return (
            StatusCode::CONFLICT,
            Json(ErrorBody { error: conflict.to_string() }),
        );
    }
    tracing::error!(%error, "request failed");
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ErrorBody {
            error: "durable storage unavailable".into(),
        }),
    )
}
