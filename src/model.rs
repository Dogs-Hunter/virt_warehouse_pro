use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn operation() -> Operation {
        Operation { operation_id: "op-1".into(), owner_id: "owner-1".into(), sku: "sku-1".into(), delta: 1, event_version: 1 }
    }

    #[test]
    fn accepts_valid_boundary_values() {
        let mut value = operation();
        value.delta = 1_000_000_000;
        assert!(value.validate().is_ok());
    }

    #[test]
    fn rejects_invalid_business_values() {
        let mut value = operation();
        value.delta = 0;
        assert!(value.validate().is_err());
        value.delta = 1;
        value.operation_id = " op-1".into();
        assert!(value.validate().is_err());
        value.operation_id = "op-1".into();
        value.event_version = 2;
        assert!(value.validate().is_err());
    }
}
