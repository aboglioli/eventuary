use std::fmt;

use futures::future::join_all;

use crate::error::{Error, Result};
use crate::event::Event;
use crate::io::{ArcWriter, Writer};

pub struct FanoutWriter {
    writers: Vec<ArcWriter>,
}

impl fmt::Debug for FanoutWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FanoutWriter")
            .field("writers", &self.writers.len())
            .finish()
    }
}

impl FanoutWriter {
    pub fn new(writers: Vec<ArcWriter>) -> Result<Self> {
        if writers.is_empty() {
            return Err(Error::Config(
                "fanout writer requires at least one writer".to_owned(),
            ));
        }
        Ok(Self { writers })
    }
}

fn first_error(results: Vec<Result<()>>) -> Result<()> {
    let mut first = None;
    for result in results {
        if let Err(error) = result
            && first.is_none()
        {
            first = Some(error);
        }
    }
    match first {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

impl Writer for FanoutWriter {
    async fn write(&self, event: &Event) -> Result<()> {
        let futures = self.writers.iter().map(|writer| writer.write(event));
        first_error(join_all(futures).await)
    }

    async fn write_all(&self, events: &[Event]) -> Result<()> {
        let futures = self.writers.iter().map(|writer| writer.write_all(events));
        first_error(join_all(futures).await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use crate::io::WriterExt;
    use crate::payload::Payload;

    #[derive(Clone, Default)]
    struct CapturingWriter {
        events: Arc<Mutex<Vec<Event>>>,
        write_all_calls: Arc<AtomicUsize>,
    }

    impl CapturingWriter {
        fn events(&self) -> Vec<Event> {
            self.events.lock().unwrap().clone()
        }

        fn write_all_calls(&self) -> usize {
            self.write_all_calls.load(Ordering::SeqCst)
        }
    }

    impl Writer for CapturingWriter {
        async fn write(&self, event: &Event) -> Result<()> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }

        async fn write_all(&self, events: &[Event]) -> Result<()> {
            self.write_all_calls.fetch_add(1, Ordering::SeqCst);
            self.events.lock().unwrap().extend(events.iter().cloned());
            Ok(())
        }
    }

    struct FailingWriter;

    impl Writer for FailingWriter {
        async fn write(&self, _: &Event) -> Result<()> {
            Err(Error::Store("writer failed".to_owned()))
        }
    }

    #[derive(Clone)]
    struct SlowWriter {
        started: Arc<AtomicUsize>,
        finished: Arc<AtomicUsize>,
    }

    impl Writer for SlowWriter {
        async fn write(&self, _: &Event) -> Result<()> {
            self.started.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            self.finished.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn ev(topic: &str) -> Event {
        Event::create("org", "/x", topic, Payload::from_string("p")).unwrap()
    }

    #[test]
    fn fanout_writer_rejects_empty_destinations() {
        let err = FanoutWriter::new(Vec::new()).unwrap_err();

        assert!(err.to_string().contains("at least one writer"));
    }

    #[tokio::test]
    async fn fanout_writer_writes_to_all_destinations() {
        let first = CapturingWriter::default();
        let second = CapturingWriter::default();
        let first_seen = first.clone();
        let second_seen = second.clone();
        let writer = FanoutWriter::new(vec![first.into_arced(), second.into_arced()]).unwrap();

        writer.write(&ev("source.topic")).await.unwrap();

        assert_eq!(first_seen.events().len(), 1);
        assert_eq!(second_seen.events().len(), 1);
        assert_eq!(first_seen.events()[0].topic().as_str(), "source.topic");
        assert_eq!(second_seen.events()[0].topic().as_str(), "source.topic");
    }

    #[tokio::test]
    async fn fanout_writer_write_all_uses_all_inner_write_all_methods() {
        let first = CapturingWriter::default();
        let second = CapturingWriter::default();
        let first_seen = first.clone();
        let second_seen = second.clone();
        let writer = FanoutWriter::new(vec![first.into_arced(), second.into_arced()]).unwrap();

        writer.write_all(&[ev("one"), ev("two")]).await.unwrap();

        assert_eq!(first_seen.write_all_calls(), 1);
        assert_eq!(second_seen.write_all_calls(), 1);
        assert_eq!(first_seen.events().len(), 2);
        assert_eq!(second_seen.events().len(), 2);
    }

    #[tokio::test]
    async fn fanout_writer_waits_for_all_destinations_even_when_one_fails() {
        let started = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let slow = SlowWriter {
            started: Arc::clone(&started),
            finished: Arc::clone(&finished),
        };
        let writer =
            FanoutWriter::new(vec![FailingWriter.into_arced(), slow.into_arced()]).unwrap();

        let err = writer.write(&ev("source.topic")).await.unwrap_err();

        assert!(err.to_string().contains("writer failed"));
        assert_eq!(started.load(Ordering::SeqCst), 1);
        assert_eq!(finished.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn fanout_writer_accepts_single_destination() {
        let only = CapturingWriter::default();
        let captured = only.clone();
        let writer = FanoutWriter::new(vec![only.into_arced()]).unwrap();

        writer.write(&ev("source.topic")).await.unwrap();

        assert_eq!(captured.events().len(), 1);
    }

    #[tokio::test]
    async fn fanout_writer_write_all_returns_first_error_after_awaiting_all() {
        let healthy = CapturingWriter::default();
        let healthy_seen = healthy.clone();
        let writer =
            FanoutWriter::new(vec![FailingWriter.into_arced(), healthy.into_arced()]).unwrap();

        let err = writer.write_all(&[ev("a"), ev("b")]).await.unwrap_err();

        assert!(err.to_string().contains("writer failed"));
        assert_eq!(healthy_seen.events().len(), 2);
        assert_eq!(healthy_seen.write_all_calls(), 1);
    }

    #[tokio::test]
    async fn fanout_writer_returns_first_error_when_all_fail() {
        let writer =
            FanoutWriter::new(vec![FailingWriter.into_arced(), FailingWriter.into_arced()])
                .unwrap();

        let err = writer.write(&ev("source.topic")).await.unwrap_err();

        assert!(err.to_string().contains("writer failed"));
    }

    #[tokio::test]
    async fn fanout_writer_debug_includes_writer_count() {
        let writer = FanoutWriter::new(vec![
            CapturingWriter::default().into_arced(),
            CapturingWriter::default().into_arced(),
            CapturingWriter::default().into_arced(),
        ])
        .unwrap();

        let debug = format!("{:?}", writer);
        assert!(debug.contains("FanoutWriter"));
        assert!(debug.contains("3"));
    }
}
