use anyhow::{Context, Result};
use async_nats::jetstream;
use sha2::{Digest, Sha256};

#[tokio::main]
async fn main() -> Result<()> {
    let command = std::env::args().nth(1).context("command is required")?;
    if command == "expect-auth-rejected" {
        let unauthorized = std::env::args().nth(2).unwrap_or_else(|| "nats://nats-1:4222".into());
        match tokio::time::timeout(std::time::Duration::from_secs(3), async_nats::connect(unauthorized)).await {
            Ok(Err(_)) => { println!("authentication_rejected"); return Ok(()); }
            Err(_) => { println!("authentication_rejected"); return Ok(()); }
            Ok(Ok(_)) => anyhow::bail!("unauthenticated NATS connection was accepted"),
        }
    }
    if command == "write-legacy-snapshot" {
        let operation_id = std::env::args().nth(2).context("operation_id is required")?;
        let owner_id = std::env::args().nth(3).context("owner_id is required")?;
        let sku = std::env::args().nth(4).context("sku is required")?;
        let delta = std::env::args().nth(5).context("delta is required")?.parse::<i64>()?;
        let mut hasher = Sha256::new();
        for field in [owner_id.as_bytes(), sku.as_bytes(), &delta.to_le_bytes(), &1_u64.to_le_bytes()] {
            hasher.update((field.len() as u64).to_le_bytes());
            hasher.update(field);
        }
        let fingerprint: [u8; 32] = hasher.finalize().into();
        let payload = serde_json::to_vec(&serde_json::json!({
            "version": 1, "sequence": 0,
            "balances": [{"owner_id": owner_id, "sku": sku, "balance": delta}],
            "dedup": [{"operation_id": operation_id, "fingerprint": fingerprint}]
        }))?;
        let mut file = Vec::with_capacity(48 + payload.len());
        file.extend_from_slice(b"WHSNAP01");
        file.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        file.extend_from_slice(&Sha256::digest(&payload));
        file.extend_from_slice(&payload);
        std::fs::write("/data/state.snapshot", file)?;
        println!("legacy_snapshot_written");
        return Ok(());
    }
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://nats-1:4222".into());
    let user = std::env::var("WAREHOUSE_NATS_USER").context("WAREHOUSE_NATS_USER is required")?;
    let password = std::env::var("WAREHOUSE_NATS_PASSWORD").context("WAREHOUSE_NATS_PASSWORD is required")?;
    let ca = std::env::var("WAREHOUSE_NATS_CA").context("WAREHOUSE_NATS_CA is required")?;
    let client = async_nats::ConnectOptions::with_user_and_password(user, password)
        .add_root_certificates(std::path::PathBuf::from(ca)).require_tls(true).connect(url).await?;
    let context = jetstream::new(client);
    match command.as_str() {
        "publish-poison" => {
            let acknowledgement = context.publish("warehouse.operations", bytes::Bytes::from_static(b"{invalid-json"))
                .await?.await?;
            println!("{}", acknowledgement.sequence);
        }
        "publish-hex" => {
            let encoded = std::env::args().nth(2).context("hex payload is required")?;
            anyhow::ensure!(encoded.len() % 2 == 0, "hex payload has odd length");
            let payload = (0..encoded.len()).step_by(2)
                .map(|index| u8::from_str_radix(&encoded[index..index + 2], 16))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let acknowledgement = context.publish("warehouse.operations", payload.into())
                .await?.await?;
            println!("{}", acknowledgement.sequence);
        }
        "publish-stale" => {
            let epoch = std::env::args().nth(2).context("epoch is required")?.parse::<u64>()?;
            let operation_id = std::env::args().nth(3).context("operation_id is required")?;
            let owner_id = std::env::args().nth(4).context("owner_id is required")?;
            let sku = std::env::args().nth(5).context("sku is required")?;
            let delta = std::env::args().nth(6).context("delta is required")?.parse::<i64>()?;
            let payload = serde_json::to_vec(&serde_json::json!({
                "record_type": "operations", "epoch": epoch,
                "operations": [{"operation_id": operation_id, "owner_id": owner_id, "sku": sku, "delta": delta, "event_version": 1}]
            }))?;
            let acknowledgement = context.publish("warehouse.operations", payload.into()).await?.await?;
            println!("{}", acknowledgement.sequence);
        }
        "dlq-count" => {
            let mut stream = context.get_stream("WAREHOUSE_DLQ").await?;
            println!("{}", stream.info().await?.state.messages);
        }
        _ => anyhow::bail!("unknown command: {command}"),
    }
    Ok(())
}
