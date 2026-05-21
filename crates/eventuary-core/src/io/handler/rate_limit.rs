use std::num::NonZeroU32;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::error::Result;
use crate::event::Event;
use crate::io::Handler;

#[derive(Debug, Clone, Copy)]
pub enum HandlerRateLimit {
    CallsPerSec(NonZeroU32),
}

impl HandlerRateLimit {
    fn interval(self) -> Duration {
        match self {
            Self::CallsPerSec(limit) => Duration::from_secs_f64(1.0 / limit.get() as f64),
        }
    }
}

pub struct RateLimitHandler<H> {
    inner: H,
    interval: Duration,
    next_allowed: Mutex<Option<Instant>>,
}

impl<H> RateLimitHandler<H> {
    pub fn new(inner: H, rate: HandlerRateLimit) -> Self {
        Self {
            inner,
            interval: rate.interval(),
            next_allowed: Mutex::new(None),
        }
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }
}

impl<H> Handler for RateLimitHandler<H>
where
    H: Handler,
{
    fn id(&self) -> &str {
        self.inner.id()
    }

    async fn handle(&self, event: &Event) -> Result<()> {
        let mut guard = self.next_allowed.lock().await;
        if let Some(deadline) = *guard {
            tokio::time::sleep_until(deadline).await;
        }
        *guard = Some(Instant::now() + self.interval);
        drop(guard);

        self.inner.handle(event).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::payload::Payload;

    struct CountingHandler {
        count: Arc<AtomicUsize>,
    }

    impl Handler for CountingHandler {
        fn id(&self) -> &str {
            "counting"
        }

        async fn handle(&self, _: &Event) -> Result<()> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn ev() -> Event {
        Event::create("org", "/x", "thing.happened", Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn rate_limit_handler_limits_calls() {
        let count = Arc::new(AtomicUsize::new(0));
        let handler = RateLimitHandler::new(
            CountingHandler {
                count: Arc::clone(&count),
            },
            HandlerRateLimit::CallsPerSec(NonZeroU32::new(100).unwrap()),
        );

        let started = Instant::now();
        for _ in 0..5 {
            handler.handle(&ev()).await.unwrap();
        }
        let elapsed = started.elapsed();

        assert_eq!(handler.id(), "counting");
        assert_eq!(count.load(Ordering::SeqCst), 5);
        assert!(
            elapsed >= Duration::from_millis(35),
            "rate limit too fast: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn rate_limit_handler_first_call_runs_immediately() {
        let count = Arc::new(AtomicUsize::new(0));
        let handler = RateLimitHandler::new(
            CountingHandler {
                count: Arc::clone(&count),
            },
            HandlerRateLimit::CallsPerSec(NonZeroU32::new(1).unwrap()),
        );

        let started = Instant::now();
        handler.handle(&ev()).await.unwrap();
        let elapsed = started.elapsed();

        assert!(elapsed < Duration::from_millis(100));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}
