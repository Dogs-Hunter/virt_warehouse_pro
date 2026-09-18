use std::{env, sync::Arc, time::{Duration, Instant}};

use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::{sync::Semaphore, task::JoinSet};

#[derive(Serialize)]
struct Operation {
    operation_id: String,
    owner_id: String,
    sku: String,
    delta: i64,
    event_version: u64,
}

#[derive(Deserialize)]
struct ApplyResult {
    status: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let url = value("WAREHOUSE_URL", "http://127.0.0.1:8080");
    let total = number("LOAD_OPERATIONS", 1_000_000)?;
    let batch_size = number("LOAD_BATCH_SIZE", 1_000)?;
    let concurrency = number("LOAD_CONCURRENCY", 32)?;
    let owners = number("LOAD_OWNERS", 10_000)?;
    let skus = number("LOAD_SKUS", 1_000)?;
    let run_id = value("LOAD_RUN_ID", "baseline-1");
    let max_retries = number("LOAD_RETRIES", 0)?;
    let retry_ms = number("LOAD_RETRY_MS", 50)?;
    let api_token = std::env::var("WAREHOUSE_API_TOKEN").ok().filter(|value| !value.is_empty());
    anyhow::ensure!(
        total > 0 && batch_size > 0 && concurrency > 0,
        "LOAD_OPERATIONS, LOAD_BATCH_SIZE and LOAD_CONCURRENCY must be positive"
    );

    let client = Client::builder()
        .pool_max_idle_per_host(concurrency)
        .tcp_nodelay(true)
        .build()?;
    let permits = Arc::new(Semaphore::new(concurrency));
    let started = Instant::now();
    let mut tasks = JoinSet::new();

    for start in (0..total).step_by(batch_size) {
        let end = (start + batch_size).min(total);
        let permit = permits.clone().acquire_owned().await?;
        let client = client.clone();
        let endpoint = format!("{url}/v1/operations/batch");
        let run_id = run_id.clone();
        let api_token = api_token.clone();
        tasks.spawn(async move {
            let _permit = permit;
            let batch_started = Instant::now();
            let operations = (start..end)
                .map(|index| Operation {
                    operation_id: format!("{run_id}-{index}"),
                    owner_id: format!("owner-{}", index % owners),
                    sku: format!("sku-{}", index % skus),
                    delta: 1,
                    event_version: 1,
                })
                .collect::<Vec<_>>();
            let mut retries = 0_usize;
            let results = loop {
                let mut request = client.post(&endpoint).json(&operations);
                if let Some(token) = &api_token { request = request.header("X-API-Key", token); }
                match request.send().await {
                    Ok(response) if response.status().is_success() => break response.json::<Vec<ApplyResult>>().await?,
                    Ok(response) if !response.status().is_server_error() => {
                        anyhow::bail!("HTTP status {} for {}", response.status(), endpoint);
                    }
                    Ok(_) | Err(_) if retries < max_retries => {
                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(retry_ms as u64)).await;
                    }
                    Ok(response) => anyhow::bail!("HTTP status {} after {} retries", response.status(), retries),
                    Err(error) => return Err(anyhow::anyhow!("request failed after {retries} retries: {error}")),
                }
            };
            let applied = results.iter().filter(|item| item.status == "applied").count();
            let duplicate = results.len() - applied;
            Ok::<_, anyhow::Error>((results.len(), applied, duplicate, batch_started.elapsed().as_micros() as u64, retries))
        });
    }

    let mut completed = 0_usize;
    let mut applied = 0_usize;
    let mut duplicates = 0_usize;
    let mut latencies_us = Vec::new();
    let mut retried_requests = 0_usize;
    while let Some(result) = tasks.join_next().await {
        let (batch_completed, batch_applied, batch_duplicates, latency_us, retries) = result??;
        completed += batch_completed;
        applied += batch_applied;
        duplicates += batch_duplicates;
        latencies_us.push(latency_us);
        retried_requests += retries;
    }
    latencies_us.sort_unstable();
    let elapsed = started.elapsed();
    println!("WAREHOUSE LOAD RESULT");
    println!("operations={completed}");
    println!("applied={applied}");
    println!("duplicates={duplicates}");
    println!("elapsed_seconds={:.3}", elapsed.as_secs_f64());
    println!("operations_per_second={:.1}", completed as f64 / elapsed.as_secs_f64());
    println!("batch_p50_ms={:.3}", percentile(&latencies_us, 50) as f64 / 1000.0);
    println!("batch_p95_ms={:.3}", percentile(&latencies_us, 95) as f64 / 1000.0);
    println!("batch_p99_ms={:.3}", percentile(&latencies_us, 99) as f64 / 1000.0);
    println!("retried_requests={retried_requests}");
    Ok(())
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    if values.is_empty() { return 0; }
    let index = ((values.len() - 1) * percentile) / 100;
    values[index]
}

fn value(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn number(name: &str, default: usize) -> Result<usize> {
    value(name, &default.to_string())
        .parse()
        .with_context(|| format!("invalid {name}"))
}
