use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;

use crate::error::Result;
use crate::event::Event;
use crate::payload::Payload;

pub trait Handler<P = Payload>: Send + Sync
where
    P: Send + Sync,
{
    fn id(&self) -> &str;
    fn handle<'a>(&'a self, event: &'a Event<P>) -> impl Future<Output = Result<()>> + Send + 'a;
}

impl<T: Handler<P> + ?Sized, P: Send + Sync> Handler<P> for Arc<T> {
    fn id(&self) -> &str {
        (**self).id()
    }
    fn handle<'a>(&'a self, event: &'a Event<P>) -> impl Future<Output = Result<()>> + Send + 'a {
        (**self).handle(event)
    }
}

impl<T: Handler<P> + ?Sized, P: Send + Sync> Handler<P> for Box<T> {
    fn id(&self) -> &str {
        (**self).id()
    }
    fn handle<'a>(&'a self, event: &'a Event<P>) -> impl Future<Output = Result<()>> + Send + 'a {
        (**self).handle(event)
    }
}

pub trait DynHandler<P = Payload>: Send + Sync {
    fn id_dyn(&self) -> &str;
    fn handle_dyn<'a>(&'a self, event: &'a Event<P>) -> BoxFuture<'a, Result<()>>;
}

impl<T: Handler<P> + ?Sized, P: Send + Sync> DynHandler<P> for T {
    fn id_dyn(&self) -> &str {
        <Self as Handler<P>>::id(self)
    }
    fn handle_dyn<'a>(&'a self, event: &'a Event<P>) -> BoxFuture<'a, Result<()>> {
        Box::pin(<Self as Handler<P>>::handle(self, event))
    }
}

pub type BoxHandler<P = Payload> = Box<dyn DynHandler<P>>;
pub type ArcHandler<P = Payload> = Arc<dyn DynHandler<P>>;

impl<P: Send + Sync> Handler<P> for dyn DynHandler<P> + '_ {
    fn id(&self) -> &str {
        DynHandler::<P>::id_dyn(self)
    }
    fn handle<'a>(&'a self, event: &'a Event<P>) -> impl Future<Output = Result<()>> + Send + 'a {
        DynHandler::handle_dyn(self, event)
    }
}

pub trait HandlerExt<P = Payload>: Handler<P> + Sized + 'static
where
    P: Send + Sync,
{
    fn into_boxed(self) -> BoxHandler<P> {
        Box::new(self)
    }

    fn into_arced(self) -> ArcHandler<P> {
        Arc::new(self)
    }
}

impl<T: Handler<P> + Sized + 'static, P: Send + Sync> HandlerExt<P> for T {}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

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
        Event::create(
            "org",
            "/x",
            "thing.happened",
            crate::payload::Payload::from_string("p"),
        )
        .unwrap()
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

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct UserUpdated {
        user_id: String,
    }

    struct TypedCountingHandler {
        id: String,
        count: Arc<AtomicUsize>,
    }

    impl Handler<UserUpdated> for TypedCountingHandler {
        fn id(&self) -> &str {
            &self.id
        }

        async fn handle(&self, event: &Event<UserUpdated>) -> Result<()> {
            assert_eq!(event.payload().user_id, "u-1");
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn typed_handler_into_boxed_yields_dyn_handler() {
        let count = Arc::new(AtomicUsize::new(0));
        let handler: BoxHandler<UserUpdated> = TypedCountingHandler {
            id: "typed".to_owned(),
            count: Arc::clone(&count),
        }
        .into_boxed();

        let event = Event::create(
            "org",
            "/users",
            "user.updated",
            UserUpdated {
                user_id: "u-1".to_owned(),
            },
        )
        .unwrap();

        handler.handle(&event).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}

pub mod filtered;
pub mod inspect;
pub mod multiplexer;
pub mod rate_limit;
pub mod retry;
pub mod subscriber_work;
pub mod timeout;

pub use filtered::FilteredHandler;
pub use inspect::{InspectHandler, InspectHandlerHooks};
pub use multiplexer::{
    Multiplexer, MultiplexerBuilder, MultiplexerKey, MultiplexerStore, NoMatchPolicy,
    NoMultiplexerStore, SubscriberId,
};
pub use rate_limit::{HandlerRateLimit, RateLimitHandler};
pub use retry::{
    DeadLetterWriter, DefaultRetryPolicy, RetryAction, RetryConfig, RetryHandler, RetryPolicy,
    backoff_delay,
};
pub use subscriber_work::{
    SubscriberWorkAcker, SubscriberWorkItem, SubscriberWorkReader, SubscriberWorkRoute,
    SubscriberWorkRouter, SubscriberWorkStore, SubscriberWorkSubscription,
};
pub use timeout::TimeoutHandler;
