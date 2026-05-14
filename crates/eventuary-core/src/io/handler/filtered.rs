use crate::error::Result;
use crate::event::Event;
use crate::io::{Filter, Handler};

pub struct FilteredHandler<H, F> {
    handler: H,
    filter: F,
}

impl<H: Handler, F: Filter> FilteredHandler<H, F> {
    pub fn new(handler: H, filter: F) -> Self {
        Self { handler, filter }
    }
}

impl<H: Handler, F: Filter> Handler for FilteredHandler<H, F> {
    fn id(&self) -> &str {
        self.handler.id()
    }

    async fn handle(&self, event: &Event) -> Result<()> {
        if !self.filter.matches(event) {
            return Ok(());
        }
        self.handler.handle(event).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::payload::Payload;

    struct CountingHandler {
        id: String,
        count: Arc<AtomicUsize>,
    }

    impl Handler for CountingHandler {
        fn id(&self) -> &str {
            &self.id
        }
        async fn handle(&self, _: &Event) -> Result<()> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct AllowNothing;
    impl Filter for AllowNothing {
        fn matches(&self, _: &Event) -> bool {
            false
        }
    }

    struct AllowAll;
    impl Filter for AllowAll {
        fn matches(&self, _: &Event) -> bool {
            true
        }
    }

    fn ev() -> Event {
        Event::create("org", "/x", "thing.happened", Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn filter_pass_invokes_inner() {
        let count = Arc::new(AtomicUsize::new(0));
        let h = FilteredHandler::new(
            CountingHandler {
                id: "h".into(),
                count: Arc::clone(&count),
            },
            AllowAll,
        );
        h.handle(&ev()).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn filter_fail_skips_inner() {
        let count = Arc::new(AtomicUsize::new(0));
        let h = FilteredHandler::new(
            CountingHandler {
                id: "h".into(),
                count: Arc::clone(&count),
            },
            AllowNothing,
        );
        h.handle(&ev()).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn id_passthrough() {
        let h = FilteredHandler::new(
            CountingHandler {
                id: "inner-id".into(),
                count: Arc::new(AtomicUsize::new(0)),
            },
            AllowAll,
        );
        assert_eq!(h.id(), "inner-id");
    }
}
