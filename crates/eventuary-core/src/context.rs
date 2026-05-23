use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::field_map::FieldMap;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ContextError {
    kind: String,
    message: String,
}

impl ContextError {
    pub fn new(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
        }
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl From<Error> for ContextError {
    fn from(err: Error) -> Self {
        Self::from(&err)
    }
}

impl From<&Error> for ContextError {
    fn from(err: &Error) -> Self {
        let (kind, message) = match err {
            Error::InvalidTopic(m) => ("invalid_topic", m.clone()),
            Error::InvalidNamespace(m) => ("invalid_namespace", m.clone()),
            Error::InvalidOrganization(m) => ("invalid_organization", m.clone()),
            Error::InvalidMetadataKey(m) => ("invalid_metadata_key", m.clone()),
            Error::InvalidPayload(m) => ("invalid_payload", m.clone()),
            Error::InvalidEventKey(m) => ("invalid_event_key", m.clone()),
            Error::InvalidConsumerGroupId(m) => ("invalid_consumer_group_id", m.clone()),
            Error::InvalidOwnerId(m) => ("invalid_owner_id", m.clone()),
            Error::OwnershipLost(m) => ("ownership_lost", m.clone()),
            Error::InvalidStartFrom(m) => ("invalid_start_from", m.clone()),
            Error::InvalidCursor(m) => ("invalid_cursor", m.clone()),
            Error::Serialization(m) => ("serialization", m.clone()),
            Error::Store(m) => ("store", m.clone()),
            Error::Handler(m) => ("handler", m.clone()),
            Error::Timeout(m) => ("timeout", m.clone()),
            Error::Config(m) => ("config", m.clone()),
        };
        Self::new(kind, message)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ContextValue {
    String(String),
    Bool(bool),
    U64(u64),
    I64(i64),
    F64(f64),
    Error(ContextError),
    Json(serde_json::Value),
}

impl ContextValue {
    fn should_store(&self) -> bool {
        match self {
            ContextValue::F64(v) => v.is_finite(),
            _ => true,
        }
    }
}

impl From<String> for ContextValue {
    fn from(value: String) -> Self {
        ContextValue::String(value)
    }
}

impl From<&str> for ContextValue {
    fn from(value: &str) -> Self {
        ContextValue::String(value.to_owned())
    }
}

impl From<bool> for ContextValue {
    fn from(value: bool) -> Self {
        ContextValue::Bool(value)
    }
}

impl From<u8> for ContextValue {
    fn from(value: u8) -> Self {
        ContextValue::U64(value.into())
    }
}

impl From<u16> for ContextValue {
    fn from(value: u16) -> Self {
        ContextValue::U64(value.into())
    }
}

impl From<u32> for ContextValue {
    fn from(value: u32) -> Self {
        ContextValue::U64(value.into())
    }
}

impl From<u64> for ContextValue {
    fn from(value: u64) -> Self {
        ContextValue::U64(value)
    }
}

impl From<usize> for ContextValue {
    fn from(value: usize) -> Self {
        ContextValue::U64(value as u64)
    }
}

impl From<i8> for ContextValue {
    fn from(value: i8) -> Self {
        ContextValue::I64(value.into())
    }
}

impl From<i16> for ContextValue {
    fn from(value: i16) -> Self {
        ContextValue::I64(value.into())
    }
}

impl From<i32> for ContextValue {
    fn from(value: i32) -> Self {
        ContextValue::I64(value.into())
    }
}

impl From<i64> for ContextValue {
    fn from(value: i64) -> Self {
        ContextValue::I64(value)
    }
}

impl From<isize> for ContextValue {
    fn from(value: isize) -> Self {
        ContextValue::I64(value as i64)
    }
}

impl From<f32> for ContextValue {
    fn from(value: f32) -> Self {
        ContextValue::F64(value.into())
    }
}

impl From<f64> for ContextValue {
    fn from(value: f64) -> Self {
        ContextValue::F64(value)
    }
}

impl From<ContextError> for ContextValue {
    fn from(value: ContextError) -> Self {
        ContextValue::Error(value)
    }
}

impl From<Error> for ContextValue {
    fn from(value: Error) -> Self {
        ContextValue::Error(ContextError::from(value))
    }
}

impl From<&Error> for ContextValue {
    fn from(value: &Error) -> Self {
        ContextValue::Error(ContextError::from(value))
    }
}

impl From<serde_json::Value> for ContextValue {
    fn from(value: serde_json::Value) -> Self {
        ContextValue::Json(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Context {
    message: String,
    fields: FieldMap<ContextValue>,
}

impl Context {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            fields: FieldMap::new(),
        }
    }

    pub fn with<V: Into<ContextValue>>(mut self, key: impl Into<String>, value: V) -> Result<Self> {
        let key = key.into();
        FieldMap::<ContextValue>::validate_key(&key)?;
        let value = value.into();
        if !value.should_store() {
            return Ok(self);
        }
        self.fields = self.fields.with(key, value)?;
        Ok(self)
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn get(&self, key: &str) -> Option<&ContextValue> {
        self.fields.get(key)
    }

    pub fn has(&self, key: &str) -> bool {
        self.fields.has(key)
    }

    pub fn fields(&self) -> &FieldMap<ContextValue> {
        &self.fields
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_with_accepts_common_value_types() {
        let ctx = Context::new("processing failed")
            .with("handler_id", "billing")
            .unwrap()
            .with("retry", true)
            .unwrap()
            .with("attempt", 3u32)
            .unwrap()
            .with("offset", -5i64)
            .unwrap()
            .with("ratio", 0.5f64)
            .unwrap()
            .with("source_error", Error::Handler("boom".to_owned()))
            .unwrap();

        assert_eq!(ctx.message(), "processing failed");
        assert_eq!(
            ctx.get("handler_id"),
            Some(&ContextValue::String("billing".to_owned()))
        );
        assert_eq!(ctx.get("retry"), Some(&ContextValue::Bool(true)));
        assert_eq!(ctx.get("attempt"), Some(&ContextValue::U64(3)));
        assert_eq!(ctx.get("offset"), Some(&ContextValue::I64(-5)));
        assert_eq!(ctx.get("ratio"), Some(&ContextValue::F64(0.5)));
        assert!(matches!(
            ctx.get("source_error"),
            Some(ContextValue::Error(e)) if e.kind() == "handler" && e.message() == "boom"
        ));
        assert!(ctx.has("handler_id"));
    }

    #[test]
    fn serde_json_value_keeps_json_variant_for_any_shape() {
        let ctx = Context::new("payload")
            .with(
                "json_string",
                serde_json::Value::String("billing".to_owned()),
            )
            .unwrap()
            .with(
                "json_object",
                serde_json::json!({"nested": {"k": [1, 2, 3]}}),
            )
            .unwrap();

        assert!(matches!(
            ctx.get("json_string"),
            Some(ContextValue::Json(serde_json::Value::String(s))) if s == "billing"
        ));
        assert!(matches!(
            ctx.get("json_object"),
            Some(ContextValue::Json(serde_json::Value::Object(_)))
        ));

        let json = serde_json::to_value(&ctx).unwrap();
        assert_eq!(
            json["fields"]["json_string"],
            serde_json::json!({"type": "json", "value": "billing"})
        );
    }

    #[test]
    fn non_finite_f64_values_are_omitted() {
        let ctx = Context::new("nan")
            .with("nan_value", f64::NAN)
            .unwrap()
            .with("inf_value", f64::INFINITY)
            .unwrap()
            .with("neg_inf", f64::NEG_INFINITY)
            .unwrap()
            .with("finite", 1.5f64)
            .unwrap();

        assert!(!ctx.has("nan_value"));
        assert!(!ctx.has("inf_value"));
        assert!(!ctx.has("neg_inf"));
        assert_eq!(ctx.get("finite"), Some(&ContextValue::F64(1.5)));
        assert_eq!(ctx.get("nan_value"), None);
    }

    #[test]
    fn context_value_serialization_is_tagged() {
        let ctx = Context::new("processing failed")
            .with("handler_id", "billing")
            .unwrap()
            .with("attempt", 3u32)
            .unwrap();

        let json = serde_json::to_value(&ctx).unwrap();
        assert_eq!(
            json["message"],
            serde_json::Value::String("processing failed".to_owned())
        );
        assert_eq!(
            json["fields"]["handler_id"],
            serde_json::json!({"type": "string", "value": "billing"})
        );
        assert_eq!(
            json["fields"]["attempt"],
            serde_json::json!({"type": "u64", "value": 3})
        );
    }

    #[test]
    fn context_roundtrips_through_json() {
        let ctx = Context::new("processing failed")
            .with("handler_id", "billing")
            .unwrap()
            .with("attempt", 3u32)
            .unwrap()
            .with("ratio", 0.25f64)
            .unwrap()
            .with("source_error", Error::Store("disk full".to_owned()))
            .unwrap()
            .with("payload", serde_json::json!({"k": "v"}))
            .unwrap();

        let serialized = serde_json::to_string(&ctx).unwrap();
        let deserialized: Context = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.message(), ctx.message());
        assert_eq!(deserialized.get("handler_id"), ctx.get("handler_id"));
        assert_eq!(deserialized.get("attempt"), ctx.get("attempt"));
        assert_eq!(deserialized.get("ratio"), ctx.get("ratio"));
        assert_eq!(deserialized.get("source_error"), ctx.get("source_error"));
        assert_eq!(deserialized.get("payload"), ctx.get("payload"));
    }
}
