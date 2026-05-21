use crate::error::{Error, Result};
use crate::event::Event;
use crate::io::Writer;

pub trait InspectWriterHooks: Send + Sync {
    fn on_write(&self, _event: &Event) {}
    fn on_write_success(&self, _event: &Event) {}
    fn on_write_error(&self, _event: &Event, _error: &Error) {}
    fn on_write_all(&self, _events: &[Event]) {}
    fn on_write_all_success(&self, _events: &[Event]) {}
    fn on_write_all_error(&self, _events: &[Event], _error: &Error) {}
}

pub struct InspectWriter<W, H> {
    inner: W,
    hooks: H,
}

impl<W, H> InspectWriter<W, H> {
    pub fn new(inner: W, hooks: H) -> Self {
        Self { inner, hooks }
    }

    pub fn hooks(&self) -> &H {
        &self.hooks
    }
}

impl<W, H> Writer for InspectWriter<W, H>
where
    W: Writer,
    H: InspectWriterHooks,
{
    async fn write(&self, event: &Event) -> Result<()> {
        self.hooks.on_write(event);
        match self.inner.write(event).await {
            Ok(()) => {
                self.hooks.on_write_success(event);
                Ok(())
            }
            Err(error) => {
                self.hooks.on_write_error(event, &error);
                Err(error)
            }
        }
    }

    async fn write_all(&self, events: &[Event]) -> Result<()> {
        self.hooks.on_write_all(events);
        match self.inner.write_all(events).await {
            Ok(()) => {
                self.hooks.on_write_all_success(events);
                Ok(())
            }
            Err(error) => {
                self.hooks.on_write_all_error(events, &error);
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

    impl InspectWriterHooks for RecordingHooks {
        fn on_write(&self, event: &Event) {
            self.calls
                .lock()
                .unwrap()
                .push(format!("write:{}", event.topic().as_str()));
        }

        fn on_write_success(&self, event: &Event) {
            self.calls
                .lock()
                .unwrap()
                .push(format!("success:{}", event.topic().as_str()));
        }

        fn on_write_error(&self, event: &Event, error: &Error) {
            self.calls
                .lock()
                .unwrap()
                .push(format!("error:{}:{error}", event.topic().as_str()));
        }

        fn on_write_all(&self, events: &[Event]) {
            self.calls
                .lock()
                .unwrap()
                .push(format!("all:{}", events.len()));
        }

        fn on_write_all_success(&self, events: &[Event]) {
            self.calls
                .lock()
                .unwrap()
                .push(format!("all-success:{}", events.len()));
        }

        fn on_write_all_error(&self, events: &[Event], error: &Error) {
            self.calls
                .lock()
                .unwrap()
                .push(format!("all-error:{}:{error}", events.len()));
        }
    }

    struct OkWriter;

    impl Writer for OkWriter {
        async fn write(&self, _: &Event) -> Result<()> {
            Ok(())
        }
    }

    struct FailingWriter;

    impl Writer for FailingWriter {
        async fn write(&self, _: &Event) -> Result<()> {
            Err(Error::Store("write failed".to_owned()))
        }

        async fn write_all(&self, _: &[Event]) -> Result<()> {
            Err(Error::Store("batch failed".to_owned()))
        }
    }

    fn ev(topic: &str) -> Event {
        Event::create("org", "/x", topic, Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn inspect_writer_records_success() {
        let hooks = RecordingHooks::default();
        let writer = InspectWriter::new(OkWriter, hooks.clone());

        writer.write(&ev("thing.happened")).await.unwrap();

        assert_eq!(
            hooks.calls(),
            vec!["write:thing.happened", "success:thing.happened"]
        );
    }

    #[tokio::test]
    async fn inspect_writer_records_error() {
        let hooks = RecordingHooks::default();
        let writer = InspectWriter::new(FailingWriter, hooks.clone());

        let err = writer.write(&ev("thing.happened")).await.unwrap_err();

        assert!(err.to_string().contains("write failed"));
        assert_eq!(hooks.calls().len(), 2);
        assert!(hooks.calls()[1].contains("error:thing.happened"));
    }

    #[tokio::test]
    async fn inspect_writer_records_batch_success() {
        let hooks = RecordingHooks::default();
        let writer = InspectWriter::new(OkWriter, hooks.clone());

        writer.write_all(&[ev("one"), ev("two")]).await.unwrap();

        assert_eq!(hooks.calls(), vec!["all:2", "all-success:2"]);
    }

    #[tokio::test]
    async fn inspect_writer_records_batch_error() {
        let hooks = RecordingHooks::default();
        let writer = InspectWriter::new(FailingWriter, hooks.clone());

        let err = writer.write_all(&[ev("one"), ev("two")]).await.unwrap_err();

        assert!(err.to_string().contains("batch failed"));
        assert_eq!(hooks.calls().len(), 2);
        assert!(hooks.calls()[1].starts_with("all-error:2"));
    }
}
