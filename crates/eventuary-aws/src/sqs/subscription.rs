use std::time::Duration;

use eventuary_core::{Error, Result};

use crate::sqs::queue::SqsQueueType;

pub(crate) const MAX_MESSAGES: i32 = 10;
pub(crate) const MAX_WAIT_TIME: Duration = Duration::from_secs(20);
pub(crate) const MAX_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(43_200);

#[derive(Debug, Clone)]
pub struct SqsSubscription {
    pub queue_url: String,
    pub queue_type: SqsQueueType,
    pub wait_time: Duration,
    pub visibility_timeout: Duration,
    pub max_messages: i32,
    pub limit: Option<usize>,
}

impl SqsSubscription {
    pub fn validate(&self) -> Result<()> {
        if self.queue_url.is_empty() {
            return Err(Error::Config("queue_url must not be empty".to_owned()));
        }
        if !(1..=MAX_MESSAGES).contains(&self.max_messages) {
            return Err(Error::Config(format!(
                "max_messages must be 1..={MAX_MESSAGES}"
            )));
        }
        if self.wait_time > MAX_WAIT_TIME {
            return Err(Error::Config("wait_time max 20s".to_owned()));
        }
        if self.visibility_timeout > MAX_VISIBILITY_TIMEOUT {
            return Err(Error::Config("visibility_timeout max 12h".to_owned()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> SqsSubscription {
        SqsSubscription {
            queue_url: "q".to_owned(),
            queue_type: SqsQueueType::default(),
            wait_time: Duration::from_secs(20),
            visibility_timeout: Duration::from_secs(30),
            max_messages: 10,
            limit: None,
        }
    }

    #[test]
    fn defaults_are_valid() {
        base().validate().unwrap();
    }

    #[test]
    fn rejects_an_empty_queue_url() {
        let mut s = base();
        s.queue_url = String::new();
        assert!(matches!(s.validate(), Err(Error::Config(_))));
    }

    #[test]
    fn rejects_max_messages_outside_the_protocol_range() {
        for count in [0, 11, -1] {
            let mut s = base();
            s.max_messages = count;
            assert!(
                matches!(s.validate(), Err(Error::Config(_))),
                "max_messages {count} must be rejected"
            );
        }
    }

    #[test]
    fn rejects_wait_time_above_20s() {
        let mut s = base();
        s.wait_time = Duration::from_secs(21);
        assert!(matches!(s.validate(), Err(Error::Config(_))));
    }

    #[test]
    fn rejects_visibility_above_12h() {
        let mut s = base();
        s.visibility_timeout = Duration::from_secs(43_201);
        assert!(matches!(s.validate(), Err(Error::Config(_))));
    }

    #[test]
    fn accepts_a_limit() {
        let mut s = base();
        s.limit = Some(5);
        s.validate().unwrap();
    }
}
