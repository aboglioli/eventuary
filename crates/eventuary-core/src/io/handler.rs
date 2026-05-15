use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;

use crate::error::Result;
use crate::event::Event;

pub trait Handler: Send + Sync {
    fn id(&self) -> &str;
    fn handle<'a>(&'a self, event: &'a Event) -> impl Future<Output = Result<()>> + Send + 'a;
}

impl<T: Handler + ?Sized> Handler for Arc<T> {
    fn id(&self) -> &str {
        (**self).id()
    }
    fn handle<'a>(&'a self, event: &'a Event) -> impl Future<Output = Result<()>> + Send + 'a {
        (**self).handle(event)
    }
}

impl<T: Handler + ?Sized> Handler for Box<T> {
    fn id(&self) -> &str {
        (**self).id()
    }
    fn handle<'a>(&'a self, event: &'a Event) -> impl Future<Output = Result<()>> + Send + 'a {
        (**self).handle(event)
    }
}

pub trait DynHandler: Send + Sync {
    fn id_dyn(&self) -> &str;
    fn handle_dyn<'a>(&'a self, event: &'a Event) -> BoxFuture<'a, Result<()>>;
}

impl<T: Handler + ?Sized> DynHandler for T {
    fn id_dyn(&self) -> &str {
        <Self as Handler>::id(self)
    }
    fn handle_dyn<'a>(&'a self, event: &'a Event) -> BoxFuture<'a, Result<()>> {
        Box::pin(<Self as Handler>::handle(self, event))
    }
}

pub type BoxHandler = Box<dyn DynHandler>;
pub type ArcHandler = Arc<dyn DynHandler>;

impl Handler for dyn DynHandler + '_ {
    fn id(&self) -> &str {
        DynHandler::id_dyn(self)
    }
    fn handle<'a>(&'a self, event: &'a Event) -> impl Future<Output = Result<()>> + Send + 'a {
        DynHandler::handle_dyn(self, event)
    }
}

pub trait HandlerExt: Handler + Sized + 'static {
    fn into_boxed(self) -> BoxHandler {
        Box::new(self)
    }

    fn into_arced(self) -> ArcHandler {
        Arc::new(self)
    }
}

impl<T: Handler + 'static> HandlerExt for T {}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn ev() -> Event {
        Event::create("org", "/x", "thing.happened", Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn handler_into_boxed_yields_dyn_handler() {
        let count = Arc::new(AtomicUsize::new(0));
        let handler: BoxHandler = CountingHandler {
            id: "h".into(),
            count: Arc::clone(&count),
        }
        .into_boxed();
        assert_eq!(handler.id(), "h");
        handler.handle(&ev()).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn handler_into_arced_yields_shared_handler() {
        let count = Arc::new(AtomicUsize::new(0));
        let handler: ArcHandler = CountingHandler {
            id: "h".into(),
            count: Arc::clone(&count),
        }
        .into_arced();
        let clone = Arc::clone(&handler);
        handler.handle(&ev()).await.unwrap();
        clone.handle(&ev()).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn handler_box_blanket_passes_as_generic_handler() {
        async fn take<H: Handler>(h: H, e: &Event) {
            h.handle(e).await.unwrap();
        }
        let count = Arc::new(AtomicUsize::new(0));
        let boxed: BoxHandler = CountingHandler {
            id: "h".into(),
            count: Arc::clone(&count),
        }
        .into_boxed();
        take(boxed, &ev()).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    fn _assert_handler_dyn_safe() {
        fn _take(_: BoxHandler) {}
        fn _take_arc(_: ArcHandler) {}
    }
}

pub mod filtered;
pub mod retry;

pub use filtered::FilteredHandler;
pub use retry::{
    DeadLetterWriter, DefaultRetryPolicy, RetryAction, RetryConfig, RetryHandler, RetryPolicy,
    backoff_delay,
};
