use std::future::Future;

use crate::error::Result;
use crate::event::Event;
use crate::io::{Reader, Writer};

#[derive(Debug, Clone)]
pub struct Duplex<W, R> {
    writer: W,
    reader: R,
}

impl<W, R> Duplex<W, R> {
    pub fn new(writer: W, reader: R) -> Self {
        Self { writer, reader }
    }

    pub fn writer(&self) -> &W {
        &self.writer
    }

    pub fn reader(&self) -> &R {
        &self.reader
    }

    pub fn into_parts(self) -> (W, R) {
        (self.writer, self.reader)
    }
}

impl<W, R, P> Writer<P> for Duplex<W, R>
where
    W: Writer<P>,
    R: Send + Sync,
    P: Send + Sync,
{
    fn write<'a>(&'a self, event: &'a Event<P>) -> impl Future<Output = Result<()>> + Send + 'a {
        self.writer.write(event)
    }

    fn write_all<'a>(
        &'a self,
        events: &'a [Event<P>],
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        self.writer.write_all(events)
    }
}

impl<W, R, P> Reader<P> for Duplex<W, R>
where
    W: Send + Sync,
    R: Reader<P>,
    P: Send + Sync,
{
    type Subscription = R::Subscription;
    type Acker = R::Acker;
    type Cursor = R::Cursor;
    type Stream = R::Stream;

    fn read(
        &self,
        subscription: Self::Subscription,
    ) -> impl Future<Output = Result<Self::Stream>> + Send {
        self.reader.read(subscription)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::{Stream, StreamExt, stream};

    use crate::io::acker::NoopAcker;
    use crate::io::{Message, NoCursor};
    use crate::payload::Payload;

    #[derive(Clone)]
    struct CountingWriter {
        writes: Arc<AtomicUsize>,
    }

    impl Writer for CountingWriter {
        async fn write(&self, _: &Event) -> Result<()> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct SingleEventReader;

    impl Reader for SingleEventReader {
        type Subscription = ();
        type Acker = NoopAcker;
        type Cursor = NoCursor;
        type Stream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, NoCursor>>> + Send>>;

        async fn read(&self, _: Self::Subscription) -> Result<Self::Stream> {
            let event = Event::create("org", "/x", "thing.happened", Payload::from_string("p"))?;
            Ok(Box::pin(stream::once(async move {
                Ok(Message::new(event, NoopAcker, NoCursor))
            })))
        }
    }

    #[tokio::test]
    async fn duplex_delegates_writes_to_writer() {
        let writes = Arc::new(AtomicUsize::new(0));
        let duplex = Duplex::new(
            CountingWriter {
                writes: Arc::clone(&writes),
            },
            SingleEventReader,
        );
        let event =
            Event::create("org", "/x", "thing.happened", Payload::from_string("p")).unwrap();

        duplex.write(&event).await.unwrap();
        duplex.write_all(&[event.clone(), event]).await.unwrap();

        assert_eq!(writes.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn duplex_delegates_reads_to_reader() {
        let writes = Arc::new(AtomicUsize::new(0));
        let duplex = Duplex::new(CountingWriter { writes }, SingleEventReader);

        let mut stream = duplex.read(()).await.unwrap();
        let message = stream.next().await.unwrap().unwrap();

        assert_eq!(message.event().topic().as_str(), "thing.happened");
    }

    #[test]
    fn duplex_exposes_parts() {
        let writes = Arc::new(AtomicUsize::new(0));
        let duplex = Duplex::new(
            CountingWriter {
                writes: Arc::clone(&writes),
            },
            SingleEventReader,
        );

        assert_eq!(duplex.writer().writes.load(Ordering::SeqCst), 0);
        let (writer, _) = duplex.into_parts();
        assert_eq!(writer.writes.load(Ordering::SeqCst), 0);
    }
}
