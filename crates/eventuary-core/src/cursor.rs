use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventSequence(u64);

impl EventSequence {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn value(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventCursor {
    Sequence(EventSequence),
    Timestamp(DateTime<Utc>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_value_round_trip() {
        let seq = EventSequence::new(42);
        assert_eq!(seq.value(), 42);
    }

    #[test]
    fn sequence_ordering() {
        let lo = EventSequence::new(1);
        let hi = EventSequence::new(2);
        assert!(lo < hi);
        assert_eq!(lo, EventSequence::new(1));
    }

    #[test]
    fn cursor_variants_distinct() {
        let seq = EventCursor::Sequence(EventSequence::new(7));
        let ts = EventCursor::Timestamp(Utc::now());
        assert_ne!(seq, ts);
    }
}
