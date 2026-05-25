use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;

use crate::error::Result;
use crate::event::Event;
use crate::payload::Payload;

pub trait Writer<P = Payload>: Send + Sync
where
    P: Send + Sync,
{
    fn write<'a>(&'a self, event: &'a Event<P>) -> impl Future<Output = Result<()>> + Send + 'a;

    fn write_all<'a>(
        &'a self,
        events: &'a [Event<P>],
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        async move {
            for event in events {
                self.write(event).await?;
            }
            Ok(())
        }
    }
}

impl<T: Writer<P> + ?Sized, P: Send + Sync> Writer<P> for Arc<T> {
    fn write<'a>(&'a self, event: &'a Event<P>) -> impl Future<Output = Result<()>> + Send + 'a {
        (**self).write(event)
    }

    fn write_all<'a>(
        &'a self,
        events: &'a [Event<P>],
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        (**self).write_all(events)
    }
}

impl<T: Writer<P> + ?Sized, P: Send + Sync> Writer<P> for Box<T> {
    fn write<'a>(&'a self, event: &'a Event<P>) -> impl Future<Output = Result<()>> + Send + 'a {
        (**self).write(event)
    }

    fn write_all<'a>(
        &'a self,
        events: &'a [Event<P>],
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        (**self).write_all(events)
    }
}

pub trait DynWriter<P = Payload>: Send + Sync {
    fn write_dyn<'a>(&'a self, event: &'a Event<P>) -> BoxFuture<'a, Result<()>>;
    fn write_all_dyn<'a>(&'a self, events: &'a [Event<P>]) -> BoxFuture<'a, Result<()>>;
}

impl<T: Writer<P> + ?Sized, P: Send + Sync> DynWriter<P> for T {
    fn write_dyn<'a>(&'a self, event: &'a Event<P>) -> BoxFuture<'a, Result<()>> {
        Box::pin(<Self as Writer<P>>::write(self, event))
    }
    fn write_all_dyn<'a>(&'a self, events: &'a [Event<P>]) -> BoxFuture<'a, Result<()>> {
        Box::pin(<Self as Writer<P>>::write_all(self, events))
    }
}

pub type BoxWriter<P = Payload> = Box<dyn DynWriter<P>>;
pub type ArcWriter<P = Payload> = Arc<dyn DynWriter<P>>;

impl<P: Send + Sync> Writer<P> for dyn DynWriter<P> + '_ {
    fn write<'a>(&'a self, event: &'a Event<P>) -> impl Future<Output = Result<()>> + Send + 'a {
        DynWriter::write_dyn(self, event)
    }
    fn write_all<'a>(
        &'a self,
        events: &'a [Event<P>],
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        DynWriter::write_all_dyn(self, events)
    }
}

pub trait WriterExt<P = Payload>: Writer<P> + Sized + 'static
where
    P: Send + Sync,
{
    fn into_boxed(self) -> BoxWriter<P> {
        Box::new(self)
    }

    fn into_arced(self) -> ArcWriter<P> {
        Arc::new(self)
    }
}

impl<T: Writer<P> + Sized + 'static, P: Send + Sync> WriterExt<P> for T {}

pub mod batch;
pub mod encode;
pub mod fanout;
pub mod filtered;
pub mod flat_map;
pub mod inspect;
pub mod map;
pub mod retry;
pub mod timeout;

pub use batch::{BatchWriter, BatchWriterConfig};
pub use encode::{EncodeWriter, WriterTypedExt};
pub use fanout::FanoutWriter;
pub use filtered::FilteredWriter;
pub use flat_map::{FlatMapWriter, TryFlatMapWriter};
pub use inspect::{InspectWriter, InspectWriterHooks};
pub use map::{MapWriter, TryMapWriter};
pub use retry::{RetryWriter, RetryWriterConfig};
pub use timeout::TimeoutWriter;

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingWriter {
        writes: Arc<AtomicUsize>,
    }

    impl Writer for CountingWriter {
        async fn write(&self, _: &Event) -> Result<()> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn ev() -> Event {
        Event::create(
            "org",
            "/x",
            "thing.happened",
            "thing-1",
            crate::payload::Payload::from_string("p"),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn into_boxed_yields_dyn_writer() {
        let writes = Arc::new(AtomicUsize::new(0));
        let writer: BoxWriter = CountingWriter {
            writes: Arc::clone(&writes),
        }
        .into_boxed();
        writer.write(&ev()).await.unwrap();
        writer.write_all(&[ev(), ev()]).await.unwrap();
        assert_eq!(writes.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn into_arced_yields_shared_writer() {
        let writes = Arc::new(AtomicUsize::new(0));
        let writer: ArcWriter = CountingWriter {
            writes: Arc::clone(&writes),
        }
        .into_arced();
        let clone = Arc::clone(&writer);
        writer.write(&ev()).await.unwrap();
        clone.write(&ev()).await.unwrap();
        assert_eq!(writes.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn box_blanket_passes_as_generic_writer() {
        async fn take<W: Writer>(w: W, e: &Event) {
            w.write(e).await.unwrap();
        }
        let writes = Arc::new(AtomicUsize::new(0));
        let boxed: BoxWriter = CountingWriter {
            writes: Arc::clone(&writes),
        }
        .into_boxed();
        take(boxed, &ev()).await;
        assert_eq!(writes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn arc_blanket_passes_as_generic_writer() {
        async fn take<W: Writer>(w: W, e: &Event) {
            w.write(e).await.unwrap();
        }
        let writes = Arc::new(AtomicUsize::new(0));
        let arced: ArcWriter = CountingWriter {
            writes: Arc::clone(&writes),
        }
        .into_arced();
        take(arced, &ev()).await;
        assert_eq!(writes.load(Ordering::SeqCst), 1);
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct UserUpdated {
        user_id: String,
    }

    fn typed_ev() -> Event<UserUpdated> {
        Event::create(
            "org",
            "/users",
            "user.updated",
            "thing-1",
            UserUpdated {
                user_id: "u-1".to_owned(),
            },
        )
        .unwrap()
    }

    struct TypedCountingWriter {
        writes: Arc<AtomicUsize>,
    }

    impl Writer<UserUpdated> for TypedCountingWriter {
        async fn write(&self, _: &Event<UserUpdated>) -> Result<()> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn typed_writer_into_boxed_yields_dyn_writer() {
        let writes = Arc::new(AtomicUsize::new(0));
        let writer: BoxWriter<UserUpdated> = TypedCountingWriter {
            writes: Arc::clone(&writes),
        }
        .into_boxed();

        writer.write(&typed_ev()).await.unwrap();
        assert_eq!(writes.load(Ordering::SeqCst), 1);
    }
}
