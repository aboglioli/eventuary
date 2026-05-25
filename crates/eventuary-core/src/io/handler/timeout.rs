use std::time::Duration;

use crate::error::{Error, Result};
use crate::event::Event;
use crate::io::Handler;

pub struct TimeoutHandler<H> {
    inner: H,
    timeout: Duration,
}

impl<H> TimeoutHandler<H> {
    pub fn new(inner: H, timeout: Duration) -> Self {
        Self { inner, timeout }
    }
}

impl<H, P> Handler<P> for TimeoutHandler<H>
where
    H: Handler<P>,
    P: Send + Sync,
{
    fn id(&self) -> &str {
        self.inner.id()
    }

    async fn handle(&self, event: &Event<P>) -> Result<()> {
        tokio::time::timeout(self.timeout, self.inner.handle(event))
            .await
            .map_err(|_| {
                Error::Timeout(format!(
                    "handler {} timed out after {:?}",
                    self.inner.id(),
                    self.timeout
                ))
            })?
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

    struct SlowHandler;

    impl Handler for SlowHandler {
        fn id(&self) -> &str {
            "slow"
        }

        async fn handle(&self, _: &Event) -> Result<()> {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        }
    }

    fn ev() -> Event {
        Event::create(
            "org",
            "/x",
            "thing.happened",
            "thing-1",
            Payload::from_string("p"),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn timeout_handler_forwards_success() {
        let count = Arc::new(AtomicUsize::new(0));
        let handler = TimeoutHandler::new(
            CountingHandler {
                count: Arc::clone(&count),
            },
            Duration::from_secs(1),
        );

        handler.handle(&ev()).await.unwrap();

        assert_eq!(handler.id(), "counting");
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn timeout_handler_times_out_slow_inner() {
        let handler = TimeoutHandler::new(SlowHandler, Duration::from_millis(10));

        let err = handler.handle(&ev()).await.unwrap_err();

        assert!(matches!(err, Error::Timeout(_)));
    }

    #[tokio::test]
    async fn timeout_handler_forwards_inner_error() {
        struct Failing;
        impl Handler for Failing {
            fn id(&self) -> &str {
                "failing"
            }
            async fn handle(&self, _: &Event) -> Result<()> {
                Err(Error::Handler("boom".to_owned()))
            }
        }

        let handler = TimeoutHandler::new(Failing, Duration::from_secs(1));
        let err = handler.handle(&ev()).await.unwrap_err();
        assert!(err.to_string().contains("boom"));
    }
}
