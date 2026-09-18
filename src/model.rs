use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Operation {
    pub operation_id: String,
    pub owner_id: String,
    pub sku: String,
    pub delta: i64,
    #[serde(default = "default_event_version")]
    pub event_version: u64,
}

impl Operation {
    pub fn validate(&self) -> Result<(), &'static str> {
        validate_identifier("operation_id", &self.operation_id)?;
        validate_identifier("owner_id", &self.owner_id)?;
        validate_identifier("sku", &self.sku)?;
        if self.delta == 0 {
            return Err("delta must not be zero");
        }
        if self.delta == i64::MIN || self.delta.abs() > 1_000_000_000 {
            return Err("delta absolute value must be <= 1000000000");
        }
        if self.event_version != 1 {
            return Err("unsupported event_version");
        }
        Ok(())
    }
}

fn validate_identifier(_field: &'static str, value: &str) -> Result<(), &'static str> {
    if value.is_empty() || value.len() > 128 {
        return Err("identifier must contain 1..=128 bytes");
    }
    if value.trim() != value || value.chars().any(char::is_control) {
        return Err("identifier must not contain surrounding whitespace or control characters");
    }
    Ok(())
}

const fn default_event_version() -> u64 { 1 }

#[derive(Debug, Serialize)]
pub struct ApplyResult {
    pub operation_id: String,
    pub status: ApplyStatus,
    pub balance: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplyStatus {
    Applied,
    Duplicate,
}
