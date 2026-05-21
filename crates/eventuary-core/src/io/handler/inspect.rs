use std::time::Duration;
use std::time::Instant;

use crate::error::{Error, Result};
use crate::event::Event;
use crate::io::Handler;

pub trait InspectHandlerHooks: Send + Sync {
    fn on_start(&self, _event: &Event) {}
    fn on_success(&self, _event: &Event, _duration: Duration) {}
    fn on_error(&self, _event: &Event, _error: &Error, _duration: Duration) {}
}

pub struct InspectHandler<H, Hooks> {
    inner: H,
    hooks: Hooks,
}

impl<H, Hooks> InspectHandler<H, Hooks> {
    pub fn new(inner: H, hooks: Hooks) -> Self {
        Self { inner, hooks }
    }

    pub fn hooks(&self) -> &Hooks {
        &self.hooks
    }
}

impl<H, Hooks> Handler for InspectHandler<H, Hooks>
where
    H: Handler,
    Hooks: InspectHandlerHooks,
{
    fn id(&self) -> &str {
        self.inner.id()
    }

    async fn handle(&self, event: &Event) -> Result<()> {
        self.hooks.on_start(event);
        let started = Instant::now();
        match self.inner.handle(event).await {
            Ok(()) => {
                self.hooks.on_success(event, started.elapsed());
                Ok(())
            }
            Err(error) => {
                self.hooks.on_error(event, &error, started.elapsed());
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::Mutex;

    use crate::payload::Payload;

    #[derive(Clone, Default)]
    struct RecordingHooks {
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl RecordingHooks {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl InspectHandlerHooks for RecordingHooks {
        fn on_start(&self, event: &Event) {
            self.calls
                .lock()
                .unwrap()
                .push(format!("start:{}", event.topic().as_str()));
        }

        fn on_success(&self, event: &Event, _: Duration) {
            self.calls
                .lock()
                .unwrap()
                .push(format!("success:{}", event.topic().as_str()));
        }

        fn on_error(&self, event: &Event, error: &Error, _: Duration) {
            self.calls
                .lock()
                .unwrap()
                .push(format!("error:{}:{error}", event.topic().as_str()));
        }
    }

    struct OkHandler;

    impl Handler for OkHandler {
        fn id(&self) -> &str {
            "ok"
        }

        async fn handle(&self, _: &Event) -> Result<()> {
            Ok(())
        }
    }

    struct FailingHandler;

    impl Handler for FailingHandler {
        fn id(&self) -> &str {
            "failing"
        }

        async fn handle(&self, _: &Event) -> Result<()> {
            Err(Error::Handler("handler failed".to_owned()))
        }
    }

    fn ev() -> Event {
        Event::create("org", "/x", "thing.happened", Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn inspect_handler_records_success() {
        let hooks = RecordingHooks::default();
        let handler = InspectHandler::new(OkHandler, hooks.clone());

        handler.handle(&ev()).await.unwrap();

        assert_eq!(
            hooks.calls(),
            vec!["start:thing.happened", "success:thing.happened"]
        );
        assert_eq!(handler.id(), "ok");
    }

    #[tokio::test]
    async fn inspect_handler_records_error() {
        let hooks = RecordingHooks::default();
        let handler = InspectHandler::new(FailingHandler, hooks.clone());

        let err = handler.handle(&ev()).await.unwrap_err();

        assert!(err.to_string().contains("handler failed"));
        assert_eq!(hooks.calls().len(), 2);
        assert!(hooks.calls()[1].contains("error:thing.happened"));
    }
}
