use std::{env, fs::File, io::{BufRead, BufReader, BufWriter, Write}, sync::Arc, time::{Duration, Instant}};

use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::{sync::Semaphore, task::JoinSet};

#[derive(Clone, Deserialize, Serialize)]
struct Operation { operation_id: String, owner_id: String, sku: String, delta: i64, event_version: u64 }

#[derive(Clone, Deserialize, Serialize)]
struct BalanceKey { owner_id: String, sku: String }

#[derive(Deserialize)]
struct HistoryPage { operations: Vec<Operation>, next_cursor: Option<String> }

#[tokio::main]
async fn main() -> Result<()> {
    match value("PREPARED_MODE", "write").as_str() {
        "prepare" => prepare(),
        "write" => write_load().await,
        "read" => read_load().await,
        "mixed" => mixed_load().await,
        mode => anyhow::bail!("unsupported PREPARED_MODE '{mode}'"),
    }
}

fn prepare() -> Result<()> {
    let path = required("PREPARED_DATASET")?;
    let total = number("LOAD_OPERATIONS", 1_000_000)?;
    let batch = number("LOAD_BATCH_SIZE", 1_000)?;
    let owners = number("LOAD_OWNERS", 10_000)?;
    let skus = number("LOAD_SKUS", 1_000)?;
    let run = value("LOAD_RUN_ID", "prepared-1");
    let mut output = BufWriter::new(File::create(&path).with_context(|| format!("cannot create {path}"))?);
    for start in (0..total).step_by(batch) {
        let operations = (start..(start + batch).min(total)).map(|index| Operation {
            operation_id: format!("{run}-{index}"), owner_id: format!("owner-{}", index % owners),
            sku: format!("sku-{}", index % skus), delta: 1, event_version: 1,
        }).collect::<Vec<_>>();
        serde_json::to_writer(&mut output, &operations)?;
        output.write_all(b"\n")?;
    }
    output.flush()?;
    println!("PREPARED DATASET RESULT");
    println!("operations={total}"); println!("batch_size={batch}"); println!("dataset={path}");
    Ok(())
}

fn load_dataset() -> Result<(Vec<Arc<Vec<u8>>>, Vec<Arc<Vec<u8>>>, usize)> {
    let path = required("PREPARED_DATASET")?;
    let reader = BufReader::new(File::open(&path).with_context(|| format!("cannot open {path}"))?);
    let mut payloads = Vec::new(); let mut read_payloads = Vec::new(); let mut total = 0;
    for line in reader.lines() {
        let line = line?; if line.is_empty() { continue; }
        let operations: Vec<Operation> = serde_json::from_str(&line)?;
        total += operations.len();
        let keys = operations.into_iter().map(|operation| BalanceKey { owner_id: operation.owner_id, sku: operation.sku }).collect::<Vec<_>>();
        read_payloads.push(Arc::new(serde_json::to_vec(&keys)?));
        payloads.push(Arc::new(line.into_bytes()));
    }
    anyhow::ensure!(total > 0, "dataset is empty");
    Ok((payloads, read_payloads, total))
}

fn load_write_dataset() -> Result<(Vec<Arc<Vec<u8>>>, usize)> {
    let path = required("PREPARED_DATASET")?;
    let reader = BufReader::new(File::open(&path).with_context(|| format!("cannot open {path}"))?);
    let mut payloads = Vec::new(); let mut total = 0;
    for line in reader.lines() {
        let line = line?; if line.is_empty() { continue; }
        let operations: Vec<Operation> = serde_json::from_str(&line)?;
        total += operations.len();
        payloads.push(Arc::new(line.into_bytes()));
    }
    anyhow::ensure!(total > 0, "dataset is empty");
    Ok((payloads, total))
}

fn load_read_dataset() -> Result<(Vec<Arc<Vec<u8>>>, usize)> {
    let path = required("PREPARED_DATASET")?;
    let reader = BufReader::new(File::open(&path).with_context(|| format!("cannot open {path}"))?);
    let mut payloads = Vec::new(); let mut total = 0;
    for line in reader.lines() {
        let line = line?; if line.is_empty() { continue; }
        let operations: Vec<Operation> = serde_json::from_str(&line)?;
        total += operations.len();
        let ids = operations.into_iter().map(|operation| operation.operation_id).collect::<Vec<_>>();
        payloads.push(Arc::new(serde_json::to_vec(&ids)?));
    }
    anyhow::ensure!(total > 0, "dataset is empty");
    Ok((payloads, total))
}

async fn write_load() -> Result<()> {
    let (payloads, total) = load_write_dataset()?;
    let url = value("WAREHOUSE_URL", "http://127.0.0.1:8080");
    let concurrency = number("LOAD_CONCURRENCY", 32)?;
    let token = env::var("WAREHOUSE_API_TOKEN").ok();
    let client = client(concurrency)?; let permits = Arc::new(Semaphore::new(concurrency)); let mut tasks = JoinSet::new();
    let started = Instant::now();
    for payload in payloads {
        let permit = permits.clone().acquire_owned().await?; let client = client.clone(); let endpoint = format!("{url}/v1/operations/batch"); let token=token.clone();
        tasks.spawn(async move { let _permit = permit; let began = Instant::now(); let values=post_json(&client,&endpoint,payload.as_ref(),token.as_deref(),"write").await?; Ok::<_,anyhow::Error>((values.len(), began.elapsed().as_micros() as u64)) });
    }
    finish("WRITE", total, started, tasks).await
}

async fn read_load() -> Result<()> {
    let (_, total) = load_read_dataset()?;
    let url = value("WAREHOUSE_URL", "http://127.0.0.1:8080"); let concurrency = number("LOAD_CONCURRENCY", 64)?;
    let token = env::var("WAREHOUSE_API_TOKEN").ok();
    let client = client(concurrency)?; let endpoint=format!("{url}/v1/operations/scan");
    let started=Instant::now(); let mut completed=0usize; let mut cursor=None; let mut latencies=Vec::new();
    loop {
        let began=Instant::now();
        let page=get_page(&client,&endpoint,cursor.as_deref(),token.as_deref()).await?;
        latencies.push(began.elapsed().as_micros() as u64);
        completed += page.operations.len();
        cursor = page.next_cursor;
        if cursor.is_none() { break; }
    }
    anyhow::ensure!(completed==total,"completed {completed} instead of {total}");
    latencies.sort_unstable();let elapsed=started.elapsed();
    println!("PREPARED READ RESULT");println!("operations={completed}");println!("elapsed_seconds={:.3}",elapsed.as_secs_f64());println!("operations_per_second={:.1}",completed as f64/elapsed.as_secs_f64());
    println!("request_p50_ms={:.3}",pct(&latencies,50)as f64/1000.0);println!("request_p95_ms={:.3}",pct(&latencies,95)as f64/1000.0);println!("request_p99_ms={:.3}",pct(&latencies,99)as f64/1000.0);Ok(())
}

async fn get_page(client:&Client,endpoint:&str,cursor:Option<&str>,token:Option<&str>)->Result<HistoryPage>{
    for attempt in 0..=6 {
        let mut request=client.get(endpoint).query(&[("limit","10000")]);
        if let Some(cursor)=cursor{request=request.query(&[("cursor",cursor)]);}
        if let Some(token)=token{request=request.header("X-API-Key",token);}
        match request.send().await {
            Ok(response) if response.status().is_success()=>return Ok(response.json().await?),
            Ok(response)=>{let status=response.status();let retry=matches!(status.as_u16(),502|503|504);let body=response.text().await.unwrap_or_default();if !retry||attempt==6{anyhow::bail!("scan HTTP {status}: {body}");}}
            Err(error)=>if attempt==6{return Err(error.into())},
        }
        tokio::time::sleep(Duration::from_millis(25_u64<<attempt)).await;
    }
    unreachable!()
}

async fn mixed_load() -> Result<()> {
    let (write_source, read_source, total) = load_dataset()?;
    let write_target = total / 5;
    let read_target = total - write_target;
    let run = value("LOAD_RUN_ID", "mixed-1");
    let mut write_payloads = Vec::new(); let mut write_operations = 0;
    for payload in write_source {
        if write_operations >= write_target { break; }
        let mut operations: Vec<Operation> = serde_json::from_slice(payload.as_ref())?;
        if write_operations + operations.len() > write_target { operations.truncate(write_target-write_operations); }
        for operation in &mut operations { operation.operation_id = format!("{run}-{}", operation.operation_id); }
        write_operations += operations.len(); write_payloads.push(Arc::new(serde_json::to_vec(&operations)?));
    }
    let mut reads = Vec::new(); let mut read_operations = 0;
    for payload in read_source {
        if read_operations >= read_target { break; }
        let mut keys: Vec<BalanceKey> = serde_json::from_slice(payload.as_ref())?;
        if read_operations + keys.len() > read_target { keys.truncate(read_target-read_operations); }
        read_operations += keys.len(); reads.push(Arc::new(serde_json::to_vec(&keys)?));
    }
    let url=value("WAREHOUSE_URL","http://127.0.0.1:8080");let concurrency=number("LOAD_CONCURRENCY",64)?;let token=env::var("WAREHOUSE_API_TOKEN").ok();
    let client=client(concurrency)?;let permits=Arc::new(Semaphore::new(concurrency));let mut tasks=JoinSet::new();let started=Instant::now();
    let mut scheduled=Vec::with_capacity(write_payloads.len()+reads.len());let mut writes_iter=write_payloads.into_iter();let mut reads_iter=reads.into_iter();
    loop { let mut added=false;for _ in 0..4{if let Some(payload)=reads_iter.next(){scheduled.push((false,payload));added=true;}}if let Some(payload)=writes_iter.next(){scheduled.push((true,payload));added=true;}if !added{break;} }
    for (is_write,payload) in scheduled {
        let permit=permits.clone().acquire_owned().await?;let client=client.clone();let token=token.clone();let endpoint=if is_write{format!("{url}/v1/operations/batch")}else{format!("{url}/v1/balances/read-batch")};
        tasks.spawn(async move{let _permit=permit;let began=Instant::now();let kind=if is_write{"mixed write"}else{"mixed read"};let values=post_json(&client,&endpoint,payload.as_ref(),token.as_deref(),kind).await?;Ok::<_,anyhow::Error>((values.len(),is_write,began.elapsed().as_micros() as u64))});
    }
    let mut writes=0;let mut read=0;let mut latencies=Vec::new();while let Some(result)=tasks.join_next().await{let(count,is_write,latency)=result??;if is_write{writes+=count}else{read+=count};latencies.push(latency);}latencies.sort_unstable();
    anyhow::ensure!(writes==write_target&&read==read_target,"mixed counts differ: writes={writes}, reads={read}");let elapsed=started.elapsed();
    println!("PREPARED MIXED RESULT");println!("operations={}",writes+read);println!("writes={writes}");println!("reads={read}");println!("elapsed_seconds={:.3}",elapsed.as_secs_f64());println!("operations_per_second={:.1}",(writes+read)as f64/elapsed.as_secs_f64());println!("request_p50_ms={:.3}",pct(&latencies,50)as f64/1000.0);println!("request_p95_ms={:.3}",pct(&latencies,95)as f64/1000.0);println!("request_p99_ms={:.3}",pct(&latencies,99)as f64/1000.0);Ok(())
}

async fn finish(kind:&str, expected:usize, started:Instant, mut tasks:JoinSet<Result<(usize,u64)>>) -> Result<()> {
    let mut completed=0; let mut latencies=Vec::new(); while let Some(result)=tasks.join_next().await { let (count,latency)=result??; completed+=count; latencies.push(latency); }
    anyhow::ensure!(completed==expected, "completed {completed} instead of {expected}"); latencies.sort_unstable(); let elapsed=started.elapsed();
    println!("PREPARED {kind} RESULT"); println!("operations={completed}"); println!("elapsed_seconds={:.3}",elapsed.as_secs_f64()); println!("operations_per_second={:.1}",completed as f64/elapsed.as_secs_f64());
    println!("request_p50_ms={:.3}",pct(&latencies,50) as f64/1000.0); println!("request_p95_ms={:.3}",pct(&latencies,95) as f64/1000.0); println!("request_p99_ms={:.3}",pct(&latencies,99) as f64/1000.0); Ok(())
}

fn client(concurrency:usize)->Result<Client>{ Ok(Client::builder().pool_max_idle_per_host(concurrency).tcp_nodelay(true).build()?) }
async fn post_json(client:&Client,endpoint:&str,payload:&[u8],token:Option<&str>,kind:&str)->Result<Vec<serde_json::Value>>{
    let retry_seconds=env::var("LOAD_RETRY_SECONDS").ok().and_then(|value|value.parse::<u64>().ok()).unwrap_or(15);
    let retry_deadline=Instant::now()+Duration::from_secs(retry_seconds);
    let regional_rtt_ms=env::var("LOAD_RTT_MS").ok().and_then(|value|value.parse::<u64>().ok()).unwrap_or(0);
    let mut attempt=0_u32;
    loop {
        if regional_rtt_ms>0 { tokio::time::sleep(Duration::from_millis(regional_rtt_ms)).await; }
        let mut request=client.post(endpoint).header("content-type","application/json").body(payload.to_vec());
        if let Some(token)=token{request=request.header("X-API-Key",token);}
        match request.send().await {
            Ok(response) if response.status().is_success()=>return Ok(response.json().await?),
            Ok(response)=>{let status=response.status();let retry=matches!(status.as_u16(),502|503|504);let body=response.text().await.unwrap_or_default();if !retry||Instant::now()>=retry_deadline{anyhow::bail!("{kind} HTTP {status}: {body}");}}
            Err(error)=>if Instant::now()>=retry_deadline{return Err(error.into())},
        }
        let delay_ms=(25_u64.saturating_mul(1_u64<<attempt.min(4))).min(500);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        attempt=attempt.saturating_add(1);
    }
}
fn pct(values:&[u64], p:usize)->u64 { if values.is_empty(){0}else{values[(values.len()-1)*p/100]} }
fn value(name:&str, default:&str)->String { env::var(name).unwrap_or_else(|_|default.to_owned()) }
fn required(name:&str)->Result<String>{ env::var(name).with_context(||format!("{name} is required")) }
fn number(name:&str, default:usize)->Result<usize>{ value(name,&default.to_string()).parse().with_context(||format!("invalid {name}")) }
