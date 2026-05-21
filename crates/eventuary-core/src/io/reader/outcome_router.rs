use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use futures::Stream;

use crate::error::Result;
use crate::event::Event;
use crate::io::{Acker, ArcWriter, Message, Reader, Writer, WriterExt};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum NackDisposition {
    #[default]
    NackInner,
    AckInnerAfterRoute,
}

pub struct OutcomeRouterReader<R> {
    inner: R,
    ack_writer: Option<ArcWriter>,
    nack_writer: Option<ArcWriter>,
    nack_disposition: NackDisposition,
}

pub struct OutcomeRouterAcker<A: Acker> {
    inner: A,
    event: Arc<Event>,
    ack_writer: Option<ArcWriter>,
    nack_writer: Option<ArcWriter>,
    nack_disposition: NackDisposition,
    completed: Arc<AtomicBool>,
}

pub struct OutcomeRouterStream<R: Reader> {
    inner: Pin<Box<R::Stream>>,
    ack_writer: Option<ArcWriter>,
    nack_writer: Option<ArcWriter>,
    nack_disposition: NackDisposition,
}

impl<R> OutcomeRouterReader<R> {
    pub fn on_ack<W>(inner: R, ack_writer: W) -> Self
    where
        W: Writer + 'static,
    {
        Self {
            inner,
            ack_writer: Some(ack_writer.into_arced()),
            nack_writer: None,
            nack_disposition: NackDisposition::NackInner,
        }
    }

    pub fn on_nack<W>(inner: R, nack_writer: W) -> Self
    where
        W: Writer + 'static,
    {
        Self {
            inner,
            ack_writer: None,
            nack_writer: Some(nack_writer.into_arced()),
            nack_disposition: NackDisposition::NackInner,
        }
    }

    pub fn on_ack_and_nack<W1, W2>(inner: R, ack_writer: W1, nack_writer: W2) -> Self
    where
        W1: Writer + 'static,
        W2: Writer + 'static,
    {
        Self {
            inner,
            ack_writer: Some(ack_writer.into_arced()),
            nack_writer: Some(nack_writer.into_arced()),
            nack_disposition: NackDisposition::NackInner,
        }
    }

    pub fn with_nack_disposition(mut self, disposition: NackDisposition) -> Self {
        self.nack_disposition = disposition;
        self
    }
}

impl<A: Acker> OutcomeRouterAcker<A> {
    fn mark_started(&self) -> bool {
        !self.completed.swap(true, Ordering::AcqRel)
    }

    async fn best_effort_nack_inner(&self) {
        let _ = self.inner.nack().await;
    }
}

impl<A> Acker for OutcomeRouterAcker<A>
where
    A: Acker + Send + Sync + 'static,
{
    async fn ack(&self) -> Result<()> {
        if !self.mark_started() {
            return Ok(());
        }

        if let Some(writer) = self.ack_writer.as_ref()
            && let Err(error) = writer.write(&self.event).await
        {
            self.best_effort_nack_inner().await;
            return Err(error);
        }

        self.inner.ack().await
    }

    async fn nack(&self) -> Result<()> {
        if !self.mark_started() {
            return Ok(());
        }

        let Some(writer) = self.nack_writer.as_ref() else {
            return self.inner.nack().await;
        };

        if let Err(error) = writer.write(&self.event).await {
            self.best_effort_nack_inner().await;
            return Err(error);
        }

        match self.nack_disposition {
            NackDisposition::NackInner => self.inner.nack().await,
            NackDisposition::AckInnerAfterRoute => self.inner.ack().await,
        }
    }
}

impl<R> Reader for OutcomeRouterReader<R>
where
    R: Reader + Send + Sync + 'static,
    R::Subscription: Send + 'static,
    R::Acker: Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
    R::Stream: 'static,
{
    type Subscription = R::Subscription;
    type Acker = OutcomeRouterAcker<R::Acker>;
    type Cursor = R::Cursor;
    type Stream = OutcomeRouterStream<R>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = self.inner.read(subscription).await?;
        Ok(OutcomeRouterStream {
            inner: Box::pin(inner),
            ack_writer: self.ack_writer.clone(),
            nack_writer: self.nack_writer.clone(),
            nack_disposition: self.nack_disposition,
        })
    }
}

impl<R> Stream for OutcomeRouterStream<R>
where
    R: Reader,
    R::Acker: Send + Sync + 'static,
{
    type Item = Result<Message<OutcomeRouterAcker<R::Acker>, R::Cursor>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(message))) => {
                let (event, inner, cursor) = message.into_parts();
                let event = Arc::new(event);
                let acker = OutcomeRouterAcker {
                    inner,
                    event: Arc::clone(&event),
                    ack_writer: self.ack_writer.clone(),
                    nack_writer: self.nack_writer.clone(),
                    nack_disposition: self.nack_disposition,
                    completed: Arc::new(AtomicBool::new(false)),
                };
                Poll::Ready(Some(Ok(Message::new((*event).clone(), acker, cursor))))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    use futures::{StreamExt, stream};

    use crate::error::Error;
    use crate::io::NoCursor;
    use crate::payload::Payload;

    #[derive(Clone, Default)]
    struct CapturingWriter {
        events: Arc<Mutex<Vec<Event>>>,
    }

    impl CapturingWriter {
        fn events(&self) -> Vec<Event> {
            self.events.lock().unwrap().clone()
        }
    }

    impl Writer for CapturingWriter {
        async fn write(&self, event: &Event) -> Result<()> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    struct FailingWriter;

    impl Writer for FailingWriter {
        async fn write(&self, _: &Event) -> Result<()> {
            Err(Error::Store("route failed".to_owned()))
        }
    }

    #[derive(Clone, Default)]
    struct CountingAcker {
        acked: Arc<AtomicUsize>,
        nacked: Arc<AtomicUsize>,
    }

    impl Acker for CountingAcker {
        async fn ack(&self) -> Result<()> {
            self.acked.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn nack(&self) -> Result<()> {
            self.nacked.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct SingleReader {
        event: Mutex<Option<Event>>,
        acker: CountingAcker,
    }

    impl SingleReader {
        fn new(event: Event, acker: CountingAcker) -> Self {
            Self {
                event: Mutex::new(Some(event)),
                acker,
            }
        }
    }

    impl Reader for SingleReader {
        type Subscription = ();
        type Acker = CountingAcker;
        type Cursor = NoCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<CountingAcker, NoCursor>>> + Send>>;

        async fn read(&self, _: ()) -> Result<Self::Stream> {
            let event = self.event.lock().unwrap().take().expect("event available");
            let acker = self.acker.clone();
            Ok(Box::pin(stream::once(async move {
                Ok(Message::new(event, acker, NoCursor))
            })))
        }
    }

    fn ev(topic: &str) -> Event {
        Event::create("org", "/x", topic, Payload::from_string("p")).unwrap()
    }

    #[tokio::test]
    async fn routes_ack_to_ack_writer_then_acks_inner() {
        let ack_writer = CapturingWriter::default();
        let captured = ack_writer.clone();
        let acker = CountingAcker::default();
        let source = SingleReader::new(ev("source.topic"), acker.clone());
        let reader = OutcomeRouterReader::on_ack(source, ack_writer);
        let mut stream = reader.read(()).await.unwrap();

        let message = stream.next().await.unwrap().unwrap();
        message.ack().await.unwrap();

        assert_eq!(captured.events().len(), 1);
        assert_eq!(captured.events()[0].topic().as_str(), "source.topic");
        assert_eq!(acker.acked.load(Ordering::SeqCst), 1);
        assert_eq!(acker.nacked.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn routes_nack_to_nack_writer_then_nacks_inner_by_default() {
        let nack_writer = CapturingWriter::default();
        let captured = nack_writer.clone();
        let acker = CountingAcker::default();
        let source = SingleReader::new(ev("source.topic"), acker.clone());
        let reader = OutcomeRouterReader::on_nack(source, nack_writer);
        let mut stream = reader.read(()).await.unwrap();

        let message = stream.next().await.unwrap().unwrap();
        message.nack().await.unwrap();

        assert_eq!(captured.events().len(), 1);
        assert_eq!(captured.events()[0].topic().as_str(), "source.topic");
        assert_eq!(acker.acked.load(Ordering::SeqCst), 0);
        assert_eq!(acker.nacked.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn routed_nack_can_ack_inner_after_route() {
        let nack_writer = CapturingWriter::default();
        let captured = nack_writer.clone();
        let acker = CountingAcker::default();
        let source = SingleReader::new(ev("source.topic"), acker.clone());
        let reader = OutcomeRouterReader::on_nack(source, nack_writer)
            .with_nack_disposition(NackDisposition::AckInnerAfterRoute);
        let mut stream = reader.read(()).await.unwrap();

        let message = stream.next().await.unwrap().unwrap();
        message.nack().await.unwrap();

        assert_eq!(captured.events().len(), 1);
        assert_eq!(acker.acked.load(Ordering::SeqCst), 1);
        assert_eq!(acker.nacked.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn nack_without_nack_writer_passes_through_even_when_ack_writer_exists() {
        let ack_writer = CapturingWriter::default();
        let captured = ack_writer.clone();
        let acker = CountingAcker::default();
        let source = SingleReader::new(ev("source.topic"), acker.clone());
        let reader = OutcomeRouterReader::on_ack(source, ack_writer)
            .with_nack_disposition(NackDisposition::AckInnerAfterRoute);
        let mut stream = reader.read(()).await.unwrap();

        let message = stream.next().await.unwrap().unwrap();
        message.nack().await.unwrap();

        assert!(captured.events().is_empty());
        assert_eq!(acker.acked.load(Ordering::SeqCst), 0);
        assert_eq!(acker.nacked.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn routes_to_both_writers_when_configured() {
        let ack_writer = CapturingWriter::default();
        let nack_writer = CapturingWriter::default();
        let ack_seen = ack_writer.clone();
        let nack_seen = nack_writer.clone();
        let first_acker = CountingAcker::default();
        let first_source = SingleReader::new(ev("source.topic"), first_acker.clone());
        let first_reader = OutcomeRouterReader::on_ack_and_nack(
            first_source,
            ack_writer.clone(),
            nack_writer.clone(),
        );
        let mut first_stream = first_reader.read(()).await.unwrap();
        first_stream
            .next()
            .await
            .unwrap()
            .unwrap()
            .ack()
            .await
            .unwrap();

        let second_acker = CountingAcker::default();
        let second_source = SingleReader::new(ev("source.topic"), second_acker.clone());
        let second_reader =
            OutcomeRouterReader::on_ack_and_nack(second_source, ack_writer, nack_writer);
        let mut second_stream = second_reader.read(()).await.unwrap();
        second_stream
            .next()
            .await
            .unwrap()
            .unwrap()
            .nack()
            .await
            .unwrap();

        assert_eq!(ack_seen.events().len(), 1);
        assert_eq!(nack_seen.events().len(), 1);
    }

    #[tokio::test]
    async fn route_failure_on_ack_best_effort_nacks_inner() {
        let acker = CountingAcker::default();
        let source = SingleReader::new(ev("source.topic"), acker.clone());
        let reader = OutcomeRouterReader::on_ack(source, FailingWriter);
        let mut stream = reader.read(()).await.unwrap();

        let err = stream
            .next()
            .await
            .unwrap()
            .unwrap()
            .ack()
            .await
            .unwrap_err();

        assert!(err.to_string().contains("route failed"));
        assert_eq!(acker.acked.load(Ordering::SeqCst), 0);
        assert_eq!(acker.nacked.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn route_failure_on_nack_best_effort_nacks_inner() {
        let acker = CountingAcker::default();
        let source = SingleReader::new(ev("source.topic"), acker.clone());
        let reader = OutcomeRouterReader::on_nack(source, FailingWriter)
            .with_nack_disposition(NackDisposition::AckInnerAfterRoute);
        let mut stream = reader.read(()).await.unwrap();

        let err = stream
            .next()
            .await
            .unwrap()
            .unwrap()
            .nack()
            .await
            .unwrap_err();

        assert!(err.to_string().contains("route failed"));
        assert_eq!(acker.acked.load(Ordering::SeqCst), 0);
        assert_eq!(acker.nacked.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn outcome_router_acker_is_single_shot() {
        let ack_writer = CapturingWriter::default();
        let captured = ack_writer.clone();
        let acker = CountingAcker::default();
        let source = SingleReader::new(ev("source.topic"), acker.clone());
        let reader = OutcomeRouterReader::on_ack(source, ack_writer);
        let mut stream = reader.read(()).await.unwrap();

        let message = stream.next().await.unwrap().unwrap();
        message.ack().await.unwrap();
        message.nack().await.unwrap();

        assert_eq!(captured.events().len(), 1);
        assert_eq!(acker.acked.load(Ordering::SeqCst), 1);
        assert_eq!(acker.nacked.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn on_ack_passes_nack_through_when_no_nack_writer() {
        let ack_writer = CapturingWriter::default();
        let captured = ack_writer.clone();
        let acker = CountingAcker::default();
        let source = SingleReader::new(ev("source.topic"), acker.clone());
        let reader = OutcomeRouterReader::on_ack(source, ack_writer);
        let mut stream = reader.read(()).await.unwrap();

        let message = stream.next().await.unwrap().unwrap();
        message.nack().await.unwrap();

        assert!(captured.events().is_empty());
        assert_eq!(acker.nacked.load(Ordering::SeqCst), 1);
        assert_eq!(acker.acked.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn nack_disposition_defaults_to_nack_inner() {
        assert_eq!(NackDisposition::default(), NackDisposition::NackInner);
    }

    struct VecReader {
        events: Mutex<Option<Vec<Event>>>,
        acker: CountingAcker,
    }

    impl VecReader {
        fn new(events: Vec<Event>, acker: CountingAcker) -> Self {
            Self {
                events: Mutex::new(Some(events)),
                acker,
            }
        }
    }

    impl Reader for VecReader {
        type Subscription = ();
        type Acker = CountingAcker;
        type Cursor = NoCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<CountingAcker, NoCursor>>> + Send>>;

        async fn read(&self, _: ()) -> Result<Self::Stream> {
            let events = self.events.lock().unwrap().take().unwrap_or_default();
            let acker = self.acker.clone();
            Ok(Box::pin(stream::iter(events.into_iter().map(move |e| {
                Ok(Message::new(e, acker.clone(), NoCursor))
            }))))
        }
    }

    #[tokio::test]
    async fn routes_every_message_in_stream_independently() {
        let nack_writer = CapturingWriter::default();
        let captured = nack_writer.clone();
        let acker = CountingAcker::default();
        let source = VecReader::new(vec![ev("a"), ev("b"), ev("c")], acker.clone());
        let reader = OutcomeRouterReader::on_nack(source, nack_writer);
        let mut stream = reader.read(()).await.unwrap();

        for _ in 0..3 {
            stream.next().await.unwrap().unwrap().nack().await.unwrap();
        }

        assert_eq!(captured.events().len(), 3);
        assert_eq!(captured.events()[0].topic().as_str(), "a");
        assert_eq!(captured.events()[1].topic().as_str(), "b");
        assert_eq!(captured.events()[2].topic().as_str(), "c");
        assert_eq!(acker.nacked.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn mixed_ack_and_nack_route_to_distinct_writers() {
        let ack_writer = CapturingWriter::default();
        let nack_writer = CapturingWriter::default();
        let ack_seen = ack_writer.clone();
        let nack_seen = nack_writer.clone();
        let acker = CountingAcker::default();
        let source = VecReader::new(vec![ev("ok.1"), ev("fail.1"), ev("ok.2")], acker.clone());
        let reader = OutcomeRouterReader::on_ack_and_nack(source, ack_writer, nack_writer);
        let mut stream = reader.read(()).await.unwrap();

        stream.next().await.unwrap().unwrap().ack().await.unwrap();
        stream.next().await.unwrap().unwrap().nack().await.unwrap();
        stream.next().await.unwrap().unwrap().ack().await.unwrap();

        let ack_topics: Vec<_> = ack_seen
            .events()
            .iter()
            .map(|e| e.topic().as_str().to_owned())
            .collect();
        let nack_topics: Vec<_> = nack_seen
            .events()
            .iter()
            .map(|e| e.topic().as_str().to_owned())
            .collect();
        assert_eq!(ack_topics, vec!["ok.1", "ok.2"]);
        assert_eq!(nack_topics, vec!["fail.1"]);
        assert_eq!(acker.acked.load(Ordering::SeqCst), 2);
        assert_eq!(acker.nacked.load(Ordering::SeqCst), 1);
    }

    struct FailingReader;

    impl Reader for FailingReader {
        type Subscription = ();
        type Acker = CountingAcker;
        type Cursor = NoCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<CountingAcker, NoCursor>>> + Send>>;

        async fn read(&self, _: ()) -> Result<Self::Stream> {
            Ok(Box::pin(stream::iter(vec![Err(Error::Store(
                "upstream broken".to_owned(),
            ))])))
        }
    }

    #[tokio::test]
    async fn propagates_inner_stream_errors_unchanged() {
        let writer = CapturingWriter::default();
        let captured = writer.clone();
        let reader = OutcomeRouterReader::on_ack(FailingReader, writer);
        let mut stream = reader.read(()).await.unwrap();

        let item = stream.next().await.unwrap();
        let err = match item {
            Ok(_) => panic!("expected stream error"),
            Err(error) => error,
        };

        assert!(err.to_string().contains("upstream broken"));
        assert!(captured.events().is_empty());
    }

    #[tokio::test]
    async fn outcome_router_composes_with_try_map_writer_and_fanout_writer() {
        use crate::io::writer::{FanoutWriter, TryMapWriter};

        let first = CapturingWriter::default();
        let second = CapturingWriter::default();
        let first_seen = first.clone();
        let second_seen = second.clone();
        let fanout = FanoutWriter::new(vec![first.into_arced(), second.into_arced()]).unwrap();
        let mapped = TryMapWriter::new(fanout, |event: &Event| {
            Event::builder(
                event.organization().as_str(),
                event.namespace().as_str(),
                "dead.letter",
                Payload::from_string(format!("failed:{}", event.id())),
            )?
            .parent_id(event.id())
            .build()
        });
        let acker = CountingAcker::default();
        let source = SingleReader::new(ev("source.topic"), acker.clone());
        let reader = OutcomeRouterReader::on_nack(source, mapped)
            .with_nack_disposition(NackDisposition::AckInnerAfterRoute);
        let mut stream = reader.read(()).await.unwrap();

        stream.next().await.unwrap().unwrap().nack().await.unwrap();

        assert_eq!(first_seen.events().len(), 1);
        assert_eq!(second_seen.events().len(), 1);
        assert_eq!(first_seen.events()[0].topic().as_str(), "dead.letter");
        assert_eq!(second_seen.events()[0].topic().as_str(), "dead.letter");
        assert_eq!(acker.acked.load(Ordering::SeqCst), 1);
        assert_eq!(acker.nacked.load(Ordering::SeqCst), 0);
    }
}
