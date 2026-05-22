use serde::{Deserialize, Serialize};

use crate::context::{Context, ContextValue};
use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NackReason {
    HandlerError,
    HandlerTimeout,
    ProcessingRejected,
    DeliveryExpired,
    RouteFailed,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NackContext {
    reason: NackReason,
    context: Context,
}

impl NackContext {
    pub fn new(reason: NackReason, context: Context) -> Self {
        Self { reason, context }
    }

    pub fn message(reason: NackReason, message: impl Into<String>) -> Self {
        Self::new(reason, Context::new(message))
    }

    pub fn handler_error(handler_id: impl Into<String>, error: Error) -> Result<Self> {
        let context = Context::new("handler failed")
            .with("handler_id", handler_id.into())?
            .with("error", error)?;
        Ok(Self::new(NackReason::HandlerError, context))
    }

    pub fn handler_timeout(
        handler_id: impl Into<String>,
        message: impl Into<String>,
    ) -> Result<Self> {
        let context = Context::new("handler timed out")
            .with("handler_id", handler_id.into())?
            .with("error", Error::Timeout(message.into()))?;
        Ok(Self::new(NackReason::HandlerTimeout, context))
    }

    pub fn delivery_expired(message: impl Into<String>) -> Self {
        Self::message(NackReason::DeliveryExpired, message)
    }

    pub fn route_failed(destination: impl Into<String>, error: Error) -> Result<Self> {
        let context = Context::new("route failed")
            .with("destination", destination.into())?
            .with("error", error)?;
        Ok(Self::new(NackReason::RouteFailed, context))
    }

    pub fn processing_rejected(message: impl Into<String>) -> Result<Self> {
        Ok(Self::message(NackReason::ProcessingRejected, message))
    }

    pub fn with<V: Into<ContextValue>>(mut self, key: impl Into<String>, value: V) -> Result<Self> {
        self.context = self.context.with(key, value)?;
        Ok(self)
    }

    pub fn reason(&self) -> NackReason {
        self.reason
    }

    pub fn context(&self) -> &Context {
        &self.context
    }
}

impl Default for NackContext {
    fn default() -> Self {
        Self::message(NackReason::Unknown, "message nacked")
    }
}

impl From<Error> for NackContext {
    fn from(error: Error) -> Self {
        let context = Context::new("message nacked")
            .with("error", error)
            .expect("valid context key");
        Self::new(NackReason::Unknown, context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handler_error_builds_context_fields() {
        let nack =
            NackContext::handler_error("billing", Error::Handler("boom".to_owned())).unwrap();

        assert_eq!(nack.reason(), NackReason::HandlerError);
        assert_eq!(nack.context().message(), "handler failed");
        assert_eq!(
            nack.context().get("handler_id"),
            Some(&ContextValue::String("billing".to_owned()))
        );
        assert!(matches!(
            nack.context().get("error"),
            Some(ContextValue::Error(e)) if e.kind() == "handler" && e.message() == "boom"
        ));
    }

    #[test]
    fn route_failed_builds_destination_and_error_fields() {
        let nack = NackContext::route_failed("dlq", Error::Store("offline".to_owned())).unwrap();

        assert_eq!(nack.reason(), NackReason::RouteFailed);
        assert_eq!(nack.context().message(), "route failed");
        assert_eq!(
            nack.context().get("destination"),
            Some(&ContextValue::String("dlq".to_owned()))
        );
        assert!(matches!(
            nack.context().get("error"),
            Some(ContextValue::Error(e)) if e.kind() == "store" && e.message() == "offline"
        ));
    }

    #[test]
    fn nack_context_roundtrips_json() {
        let nack = NackContext::handler_timeout("billing", "exceeded 5s")
            .unwrap()
            .with("attempt", 2u32)
            .unwrap();

        let serialized = serde_json::to_string(&nack).unwrap();
        let deserialized: NackContext = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.reason(), NackReason::HandlerTimeout);
        assert_eq!(deserialized.context().message(), "handler timed out");
        assert_eq!(
            deserialized.context().get("handler_id"),
            Some(&ContextValue::String("billing".to_owned()))
        );
        assert_eq!(
            deserialized.context().get("attempt"),
            Some(&ContextValue::U64(2))
        );
        assert!(matches!(
            deserialized.context().get("error"),
            Some(ContextValue::Error(e)) if e.kind() == "timeout" && e.message() == "exceeded 5s"
        ));
    }
}
