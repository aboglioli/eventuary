use eventuary_core::Event;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SqsQueueType {
    #[default]
    Standard,
    Fifo,
    FifoContentBasedDeduplication,
}

impl SqsQueueType {
    pub fn is_fifo(&self) -> bool {
        !matches!(self, Self::Standard)
    }

    pub(crate) fn message_group_id(&self, event: &Event) -> Option<String> {
        match self {
            Self::Standard => None,
            Self::Fifo | Self::FifoContentBasedDeduplication => {
                Some(event.key().as_str().to_owned())
            }
        }
    }

    pub(crate) fn message_deduplication_id(&self, event: &Event) -> Option<String> {
        match self {
            Self::Fifo => Some(event.id().to_string()),
            Self::Standard | Self::FifoContentBasedDeduplication => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventuary_core::Payload;

    fn event(key: &str) -> Event {
        Event::create(
            "acme",
            "/orders",
            "order.placed",
            key,
            Payload::from_string("v"),
        )
        .expect("valid event")
    }

    #[test]
    fn standard_sets_no_fifo_attributes() {
        let queue_type = SqsQueueType::Standard;
        let event = event("order-1");
        assert!(!queue_type.is_fifo());
        assert!(queue_type.message_group_id(&event).is_none());
        assert!(queue_type.message_deduplication_id(&event).is_none());
    }

    #[test]
    fn fifo_groups_by_event_key_and_dedupes_by_event_id() {
        let queue_type = SqsQueueType::Fifo;
        let event = event("order-1");
        assert!(queue_type.is_fifo());
        assert_eq!(
            queue_type.message_group_id(&event).as_deref(),
            Some("order-1")
        );
        let expected_dedup_id = event.id().to_string();
        assert_eq!(
            queue_type.message_deduplication_id(&event).as_deref(),
            Some(expected_dedup_id.as_str())
        );
    }

    #[test]
    fn fifo_content_based_dedup_omits_deduplication_id() {
        let queue_type = SqsQueueType::FifoContentBasedDeduplication;
        let event = event("order-1");
        assert!(queue_type.is_fifo());
        assert_eq!(
            queue_type.message_group_id(&event).as_deref(),
            Some("order-1")
        );
        assert!(queue_type.message_deduplication_id(&event).is_none());
    }

    #[test]
    fn events_sharing_a_key_share_a_message_group() {
        let queue_type = SqsQueueType::Fifo;
        let first = event("order-1");
        let second = event("order-1");
        assert_eq!(
            queue_type.message_group_id(&first),
            queue_type.message_group_id(&second)
        );
        assert_ne!(
            queue_type.message_deduplication_id(&first),
            queue_type.message_deduplication_id(&second)
        );
    }
}
