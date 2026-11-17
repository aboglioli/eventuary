use std::any::Any;
use std::panic::AssertUnwindSafe;

use futures::FutureExt;

use crate::error::{Error, Result};
use crate::event::Event;
use crate::io::Handler;

/// Turns a panic in the inner handler into [`Error::HandlerPanicked`].
///
/// Without it a panic unwinds into whatever drives the handler. With it the
/// panic is an ordinary error that a [`RetryHandler`](super::RetryHandler) can
/// classify: [`DefaultRetryPolicy`](super::DefaultRetryPolicy) dead-letters it
/// at once, because running the same event again panics again.
pub struct CatchPanicHandler<H> {
    inner: H,
}

impl<H> CatchPanicHandler<H> {
    pub fn new(inner: H) -> Self {
        Self { inner }
    }
}

impl<H, P> Handler<P> for CatchPanicHandler<H>
where
    H: Handler<P>,
    P: Send + Sync,
{
    fn id(&self) -> &str {
        self.inner.id()
    }

    async fn handle(&self, event: &Event<P>) -> Result<()> {
        AssertUnwindSafe(self.inner.handle(event))
            .catch_unwind()
            .await
            .unwrap_or_else(|panic| {
                Err(Error::HandlerPanicked(format!(
                    "handler {} panicked: {}",
                    self.inner.id(),
                    panic_message(panic.as_ref())
                )))
            })
    }
}

pub(crate) fn panic_message(panic: &(dyn Any + Send)) -> &str {
    if let Some(message) = panic.downcast_ref::<&str>() {
        return message;
    }
    if let Some(message) = panic.downcast_ref::<String>() {
        return message;
    }
    "non-string panic payload"
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::io::Writer;
    use crate::io::handler::{DeadLetterWriter, DefaultRetryPolicy, RetryConfig, RetryHandler};
    use crate::payload::Payload;

    enum Behaviour {
        Succeed,
        Fail,
        PanicWithStr,
        PanicWithString,
        PanicWithValue,
    }

    struct Scripted {
        behaviour: Behaviour,
        calls: Arc<AtomicUsize>,
    }

    impl Scripted {
        fn new(behaviour: Behaviour) -> (Self, Arc<AtomicUsize>) {
            let calls = Arc::new(AtomicUsize::new(0));
            (
                Self {
                    behaviour,
                    calls: Arc::clone(&calls),
                },
                calls,
            )
        }
    }

    impl Handler for Scripted {
        fn id(&self) -> &str {
            "scripted"
        }

        async fn handle(&self, _: &Event) -> Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.behaviour {
                Behaviour::Succeed => Ok(()),
                Behaviour::Fail => Err(Error::Store("down".to_owned())),
                Behaviour::PanicWithStr => panic!("boom"),
                Behaviour::PanicWithString => panic!("{}", String::from("formatted boom")),
                Behaviour::PanicWithValue => std::panic::panic_any(42_u32),
            }
        }
    }

    struct Capturing(Arc<std::sync::Mutex<Vec<Event>>>);

    impl Writer for Capturing {
        async fn write(&self, event: &Event) -> Result<()> {
            self.0.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    fn event() -> Event {
        Event::builder(
            "acme",
            "/x",
            "thing.happened",
            "k",
            Payload::from_string("p"),
        )
        .unwrap()
        .build()
        .unwrap()
    }

    #[tokio::test]
    async fn a_handler_that_succeeds_or_fails_is_passed_through_unchanged() {
        let (ok, _) = Scripted::new(Behaviour::Succeed);
        let (failing, _) = Scripted::new(Behaviour::Fail);

        assert!(CatchPanicHandler::new(ok).handle(&event()).await.is_ok());
        let error = CatchPanicHandler::new(failing)
            .handle(&event())
            .await
            .unwrap_err();

        assert!(matches!(error, Error::Store(_)));
    }

    #[tokio::test]
    async fn a_panic_becomes_a_handler_panicked_error_naming_the_handler_and_the_message() {
        for (behaviour, message) in [
            (Behaviour::PanicWithStr, "boom"),
            (Behaviour::PanicWithString, "formatted boom"),
            (Behaviour::PanicWithValue, "non-string panic payload"),
        ] {
            let (handler, _) = Scripted::new(behaviour);

            let error = CatchPanicHandler::new(handler)
                .handle(&event())
                .await
                .unwrap_err();

            let Error::HandlerPanicked(text) = error else {
                panic!("expected a panic error");
            };
            assert_eq!(text, format!("handler scripted panicked: {message}"));
        }
    }

    #[tokio::test]
    async fn the_wrapper_keeps_the_id_of_the_handler() {
        let (handler, _) = Scripted::new(Behaviour::Succeed);

        assert_eq!(CatchPanicHandler::new(handler).id(), "scripted");
    }

    #[tokio::test]
    async fn a_panicking_handler_is_dead_lettered_after_one_attempt_not_the_whole_budget() {
        let (handler, calls) = Scripted::new(Behaviour::PanicWithStr);
        let written = Arc::new(std::sync::Mutex::new(Vec::new()));
        let retry = RetryHandler::new(
            CatchPanicHandler::new(handler),
            DefaultRetryPolicy,
            RetryConfig::new(5, Duration::from_millis(1), Duration::from_millis(5), 2.0).unwrap(),
            DeadLetterWriter::new(Capturing(Arc::clone(&written))),
        );

        retry.handle(&event()).await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let events = written.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].topic().as_str(), "thing.happened.dead_letter");
    }

    #[tokio::test]
    async fn an_ordinary_failure_still_uses_the_whole_retry_budget() {
        let (handler, calls) = Scripted::new(Behaviour::Fail);
        let written = Arc::new(std::sync::Mutex::new(Vec::new()));
        let retry = RetryHandler::new(
            CatchPanicHandler::new(handler),
            DefaultRetryPolicy,
            RetryConfig::new(4, Duration::from_millis(1), Duration::from_millis(5), 2.0).unwrap(),
            DeadLetterWriter::new(Capturing(Arc::clone(&written))),
        );

        retry.handle(&event()).await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert_eq!(written.lock().unwrap().len(), 1);
    }
}
