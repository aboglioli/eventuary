use std::pin::Pin;
use std::sync::Arc;

use futures::{StreamExt, stream};

use crate::error::Result;
use crate::io::{BoxStream, Filter, Message, Reader};

pub type FilteredStream<C, A> = BoxStream<C, A>;

pub struct FilteredReader<R, F> {
    inner: R,
    filter: Arc<F>,
}

impl<R, F> FilteredReader<R, F> {
    pub fn new(inner: R, filter: F) -> Self {
        Self {
            inner,
            filter: Arc::new(filter),
        }
    }

    pub fn inner(&self) -> &R {
        &self.inner
    }

    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R, F> Reader for FilteredReader<R, F>
where
    R: Reader + Send + Sync + 'static,
    R::Subscription: Send,
    R::Acker: Send + Sync + 'static,
    R::Cursor: Send + Sync + 'static,
    R::Stream: 'static,
    F: Filter + 'static,
{
    type Subscription = R::Subscription;
    type Acker = R::Acker;
    type Cursor = R::Cursor;
    type Stream = FilteredStream<R::Cursor, R::Acker>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = self.inner.read(subscription).await?;
        let state = FilteredState {
            inner: Box::pin(inner),
            filter: Arc::clone(&self.filter),
            is_done: false,
        };
        Ok(Box::pin(stream::unfold(state, next_filtered::<R, F>)))
    }
}

struct FilteredState<R, F>
where
    R: Reader,
{
    inner: Pin<Box<R::Stream>>,
    filter: Arc<F>,
    is_done: bool,
}

async fn next_filtered<R, F>(
    mut state: FilteredState<R, F>,
) -> Option<(Result<Message<R::Acker, R::Cursor>>, FilteredState<R, F>)>
where
    R: Reader,
    F: Filter,
{
    if state.is_done {
        return None;
    }

    loop {
        let item = state.inner.as_mut().next().await?;
        match item {
            Ok(msg) if state.filter.matches(msg.event()) => return Some((Ok(msg), state)),
            Ok(msg) => {
                if let Err(e) = msg.ack().await {
                    state.is_done = true;
                    return Some((Err(e), state));
                }
            }
            Err(e) => return Some((Err(e), state)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use futures::Stream;
    use futures::stream;

    use super::*;
    use crate::OrganizationId;
    use crate::error::Error;
    use crate::event::Event;
    use crate::io::Acker;
    use crate::payload::Payload;

    #[derive(Clone, Default)]
    struct CountingAcker {
        ack_count: Arc<AtomicUsize>,
        nack_count: Arc<AtomicUsize>,
        fail_ack: Arc<AtomicBool>,
    }

    impl Acker for CountingAcker {
        async fn ack(&self) -> Result<()> {
            self.ack_count.fetch_add(1, Ordering::SeqCst);
            if self.fail_ack.load(Ordering::SeqCst) {
                return Err(Error::Store("ack failed".to_owned()));
            }
            Ok(())
        }

        async fn nack(&self) -> Result<()> {
            self.nack_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    struct TestSubscription;

    type TestItem = Result<Message<CountingAcker, TestCursor>>;

    struct VecReader {
        items: Mutex<Option<Vec<TestItem>>>,
    }

    impl VecReader {
        fn new(items: Vec<TestItem>) -> Self {
            Self {
                items: Mutex::new(Some(items)),
            }
        }
    }

    impl Reader for VecReader {
        type Subscription = TestSubscription;
        type Acker = CountingAcker;
        type Cursor = TestCursor;
        type Stream =
            Pin<Box<dyn Stream<Item = Result<Message<CountingAcker, TestCursor>>> + Send>>;

        async fn read(&self, _: Self::Subscription) -> Result<Self::Stream> {
            let items = self.items.lock().unwrap().take().unwrap_or_default();
            Ok(Box::pin(stream::iter(items)))
        }
    }

    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    struct TestCursor(u64);

    fn ev(org: &str, key: &str) -> Event {
        Event::builder(org, "/x", "thing.happened", Payload::from_string("p"))
            .unwrap()
            .key(key)
            .unwrap()
            .build()
            .unwrap()
    }

    fn msg(event: Event, acker: CountingAcker, cursor: u64) -> Message<CountingAcker, TestCursor> {
        Message::new(event, acker, TestCursor(cursor))
    }

    #[tokio::test]
    async fn yields_matching_events_unchanged() {
        let acker = CountingAcker::default();
        let reader = VecReader::new(vec![Ok(msg(ev("acme", "k0"), acker.clone(), 42))]);
        let filtered = FilteredReader::new(
            reader,
            crate::io::filter::EventFilter::for_organization(OrganizationId::new("acme").unwrap()),
        );

        let mut stream = filtered.read(TestSubscription).await.unwrap();
        let item = stream.next().await.unwrap().unwrap();

        assert_eq!(item.event().key().unwrap().as_str(), "k0");
        assert_eq!(*item.cursor(), TestCursor(42));
        assert_eq!(acker.ack_count.load(Ordering::SeqCst), 0);
        assert_eq!(acker.nack_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn acks_and_skips_non_matching_events() {
        let skipped = CountingAcker::default();
        let delivered = CountingAcker::default();
        let reader = VecReader::new(vec![
            Ok(msg(ev("other", "skip"), skipped.clone(), 1)),
            Ok(msg(ev("acme", "keep"), delivered.clone(), 2)),
        ]);
        let filtered = FilteredReader::new(
            reader,
            crate::io::filter::EventFilter::for_organization(OrganizationId::new("acme").unwrap()),
        );

        let mut stream = filtered.read(TestSubscription).await.unwrap();
        let item = stream.next().await.unwrap().unwrap();

        assert_eq!(item.event().key().unwrap().as_str(), "keep");
        assert_eq!(*item.cursor(), TestCursor(2));
        assert_eq!(skipped.ack_count.load(Ordering::SeqCst), 1);
        assert_eq!(skipped.nack_count.load(Ordering::SeqCst), 0);
        assert_eq!(delivered.ack_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn surfaces_skip_ack_error_and_terminates() {
        let skipped = CountingAcker::default();
        skipped.fail_ack.store(true, Ordering::SeqCst);
        let delivered = CountingAcker::default();
        let reader = VecReader::new(vec![
            Ok(msg(ev("other", "skip"), skipped.clone(), 1)),
            Ok(msg(ev("acme", "keep"), delivered.clone(), 2)),
        ]);
        let filtered = FilteredReader::new(
            reader,
            crate::io::filter::EventFilter::for_organization(OrganizationId::new("acme").unwrap()),
        );

        let mut stream = filtered.read(TestSubscription).await.unwrap();
        let err = match stream.next().await.unwrap() {
            Ok(_) => panic!("expected ack error"),
            Err(e) => e,
        };

        assert!(err.to_string().contains("ack failed"));
        assert_eq!(skipped.ack_count.load(Ordering::SeqCst), 1);
        assert_eq!(delivered.ack_count.load(Ordering::SeqCst), 0);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn yields_inner_stream_errors_unchanged() {
        let reader = VecReader::new(vec![Err(Error::Store("inner failed".to_owned()))]);
        let filtered = FilteredReader::new(
            reader,
            crate::io::filter::EventFilter::for_organization(OrganizationId::new("acme").unwrap()),
        );

        let mut stream = filtered.read(TestSubscription).await.unwrap();
        let err = match stream.next().await.unwrap() {
            Ok(_) => panic!("expected inner error"),
            Err(e) => e,
        };

        assert!(err.to_string().contains("inner failed"));
    }

    #[tokio::test]
    async fn works_with_topic_pattern_filter() {
        use crate::topic::Topic;
        use crate::topic_pattern::TopicPattern;

        let acker = CountingAcker::default();
        let reader = VecReader::new(vec![Ok(msg(ev("acme", "k0"), acker.clone(), 1))]);
        let filtered = FilteredReader::new(
            reader,
            TopicPattern::exact(Topic::new("thing.happened").unwrap()),
        );

        let mut stream = filtered.read(TestSubscription).await.unwrap();
        let item = stream.next().await.unwrap().unwrap();
        assert_eq!(item.event().key().unwrap().as_str(), "k0");
    }

    #[tokio::test]
    async fn works_with_combined_filters() {
        use crate::io::FilterExt;
        use crate::topic::Topic;
        use crate::topic_pattern::TopicPattern;

        struct OrgAcme;
        impl Filter for OrgAcme {
            fn matches(&self, e: &Event) -> bool {
                e.organization().as_str() == "acme"
            }
        }

        let acker = CountingAcker::default();
        let reader = VecReader::new(vec![Ok(msg(ev("acme", "k0"), acker.clone(), 1))]);
        let filter = OrgAcme.and(TopicPattern::exact(Topic::new("thing.happened").unwrap()));
        let filtered = FilteredReader::new(reader, filter);

        let mut stream = filtered.read(TestSubscription).await.unwrap();
        let item = stream.next().await.unwrap().unwrap();
        assert_eq!(item.event().key().unwrap().as_str(), "k0");
    }
}
