use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::io::stream::SpawnedStream;
use crate::io::{Acker, Message, Reader, StartFrom, StartableSubscription};

#[derive(Debug, Clone)]
pub struct SupervisedConfig {
    initial_backoff: Duration,
    max_backoff: Duration,
    backoff_multiplier: f64,
    healthy_after: Duration,
    max_consecutive_failures: Option<usize>,
    should_restart_on_end: bool,
}

impl SupervisedConfig {
    pub fn new(
        initial_backoff: Duration,
        max_backoff: Duration,
        backoff_multiplier: f64,
        healthy_after: Duration,
    ) -> Result<Self> {
        if !backoff_multiplier.is_finite() || backoff_multiplier < 1.0 {
            return Err(Error::Config(format!(
                "supervised backoff_multiplier must be finite and >= 1.0, got {backoff_multiplier}"
            )));
        }
        if max_backoff < initial_backoff {
            return Err(Error::Config(
                "supervised max_backoff must be >= initial_backoff".into(),
            ));
        }
        Ok(Self {
            initial_backoff,
            max_backoff,
            backoff_multiplier,
            healthy_after,
            max_consecutive_failures: None,
            should_restart_on_end: false,
        })
    }

    pub fn with_max_consecutive_failures(mut self, max: usize) -> Self {
        self.max_consecutive_failures = Some(max);
        self
    }

    pub fn with_restart_on_end(mut self) -> Self {
        self.should_restart_on_end = true;
        self
    }

    pub fn initial_backoff(&self) -> Duration {
        self.initial_backoff
    }

    pub fn max_backoff(&self) -> Duration {
        self.max_backoff
    }

    pub fn backoff_multiplier(&self) -> f64 {
        self.backoff_multiplier
    }

    pub fn healthy_after(&self) -> Duration {
        self.healthy_after
    }

    pub fn max_consecutive_failures(&self) -> Option<usize> {
        self.max_consecutive_failures
    }

    pub fn should_restart_on_end(&self) -> bool {
        self.should_restart_on_end
    }

    pub fn backoff_after(&self, consecutive_failures: usize) -> Duration {
        let exponent = i32::try_from(consecutive_failures.saturating_sub(1)).unwrap_or(i32::MAX);
        let seconds = self.initial_backoff.as_secs_f64() * self.backoff_multiplier.powi(exponent);
        if !seconds.is_finite() || seconds >= self.max_backoff.as_secs_f64() {
            return self.max_backoff;
        }
        Duration::from_secs_f64(seconds)
    }
}

impl Default for SupervisedConfig {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            healthy_after: Duration::from_secs(60),
            max_consecutive_failures: None,
            should_restart_on_end: false,
        }
    }
}

pub struct SupervisedReader<R> {
    inner: Arc<R>,
    config: SupervisedConfig,
    cancel: CancellationToken,
}

impl<R> SupervisedReader<R> {
    pub fn new(inner: R, config: SupervisedConfig) -> Self {
        Self {
            inner: Arc::new(inner),
            config,
            cancel: CancellationToken::new(),
        }
    }

    pub fn with_cancellation(mut self, cancel: CancellationToken) -> Self {
        self.cancel = cancel;
        self
    }
}

pub struct SupervisedAcker<A, C> {
    inner: A,
    cursor: C,
    last_acked: Arc<Mutex<Option<C>>>,
}

impl<A, C> Acker for SupervisedAcker<A, C>
where
    A: Acker,
    C: Clone + Send + Sync,
{
    async fn ack(&self) -> Result<()> {
        self.inner.ack().await?;
        if let Ok(mut last_acked) = self.last_acked.lock() {
            *last_acked = Some(self.cursor.clone());
        }
        Ok(())
    }

    async fn nack(&self) -> Result<()> {
        self.inner.nack().await
    }

    async fn nack_with(&self, context: crate::io::acker::NackContext) -> Result<()> {
        self.inner.nack_with(context).await
    }
}

impl<R, P> Reader<P> for SupervisedReader<R>
where
    R: Reader<P> + 'static,
    R::Subscription: StartableSubscription<R::Cursor> + 'static,
    R::Acker: 'static,
    R::Cursor: Clone + Send + Sync + 'static,
    R::Stream: 'static,
    P: Send + 'static,
{
    type Subscription = R::Subscription;
    type Acker = SupervisedAcker<R::Acker, R::Cursor>;
    type Cursor = R::Cursor;
    type Stream = SpawnedStream<Self::Acker, Self::Cursor, P>;

    async fn read(&self, subscription: Self::Subscription) -> Result<Self::Stream> {
        let inner = Arc::clone(&self.inner);
        let config = self.config.clone();
        let cancel = self.cancel.clone();
        let (tx, rx) = mpsc::channel(64);

        let handle = tokio::spawn(async move {
            let last_acked: Arc<Mutex<Option<R::Cursor>>> = Arc::new(Mutex::new(None));
            let mut consecutive_failures = 0usize;

            loop {
                let resume_from = last_acked.lock().ok().and_then(|cursor| cursor.clone());
                let session_subscription = match resume_from {
                    Some(cursor) => subscription.clone().with_start(StartFrom::After(cursor)),
                    None => subscription.clone(),
                };
                let opened_at = Instant::now();

                let failure = match inner.read(session_subscription).await {
                    Err(e) => e,
                    Ok(stream) => {
                        let mut stream = Box::pin(stream);
                        loop {
                            let item = tokio::select! {
                                _ = cancel.cancelled() => return,
                                _ = tx.closed() => return,
                                item = stream.next() => item,
                            };
                            match item {
                                Some(Ok(msg)) => {
                                    let (event, acker, cursor) = msg.into_parts();
                                    let supervised = Message::new(
                                        event,
                                        SupervisedAcker {
                                            inner: acker,
                                            cursor: cursor.clone(),
                                            last_acked: Arc::clone(&last_acked),
                                        },
                                        cursor,
                                    );
                                    tokio::select! {
                                        _ = cancel.cancelled() => return,
                                        sent = tx.send(Ok(supervised)) => {
                                            if sent.is_err() {
                                                return;
                                            }
                                        }
                                    }
                                }
                                Some(Err(e)) => break e,
                                None if config.should_restart_on_end => {
                                    break Error::Store("supervised stream ended".into());
                                }
                                None => return,
                            }
                        }
                    }
                };

                if opened_at.elapsed() >= config.healthy_after {
                    consecutive_failures = 0;
                }
                consecutive_failures += 1;
                if config
                    .max_consecutive_failures
                    .is_some_and(|max| consecutive_failures > max)
                {
                    let _ = tx.send(Err(failure)).await;
                    return;
                }
                tracing::warn!("supervised reader failure {consecutive_failures}: {failure}");

                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tx.closed() => return,
                    _ = tokio::time::sleep(config.backoff_after(consecutive_failures)) => {}
                }
            }
        });

        Ok(SpawnedStream::new(rx, handle))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::pin::Pin;

    use futures::{Stream, stream};

    use super::*;
    use crate::event::Event;
    use crate::io::acker::NoopAcker;
    use crate::payload::Payload;

    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    struct TestCursor(u64);

    #[derive(Debug, Clone, Default)]
    struct TestSub {
        after: Option<u64>,
    }

    impl StartableSubscription<TestCursor> for TestSub {
        fn with_start(mut self, start: StartFrom<TestCursor>) -> Self {
            if let StartFrom::After(cursor) = start {
                self.after = Some(cursor.0);
            }
            self
        }
    }

    #[derive(Clone)]
    enum Step {
        Message(u64),
        Sleep(Duration),
        Fail,
    }

    #[derive(Clone)]
    enum Session {
        Opens(Vec<Step>),
        RefusesToOpen,
    }

    struct ScriptedReader {
        sessions: Mutex<VecDeque<Session>>,
        opened_with: Arc<Mutex<Vec<Option<u64>>>>,
    }

    impl ScriptedReader {
        fn new(sessions: Vec<Session>) -> (Self, Arc<Mutex<Vec<Option<u64>>>>) {
            let opened_with = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    sessions: Mutex::new(sessions.into()),
                    opened_with: Arc::clone(&opened_with),
                },
                opened_with,
            )
        }
    }

    type TestStream = Pin<Box<dyn Stream<Item = Result<Message<NoopAcker, TestCursor>>> + Send>>;

    impl Reader for ScriptedReader {
        type Subscription = TestSub;
        type Acker = NoopAcker;
        type Cursor = TestCursor;
        type Stream = TestStream;

        async fn read(&self, subscription: TestSub) -> Result<Self::Stream> {
            self.opened_with.lock().unwrap().push(subscription.after);
            let session = self.sessions.lock().unwrap().pop_front();
            match session {
                None => Ok(Box::pin(stream::pending())),
                Some(Session::RefusesToOpen) => Err(Error::Store("refused".into())),
                Some(Session::Opens(steps)) => Ok(Box::pin(stream::unfold(
                    VecDeque::from(steps),
                    |mut steps| async move {
                        loop {
                            match steps.pop_front()? {
                                Step::Message(n) => {
                                    let event = Event::create(
                                        "org",
                                        "/x",
                                        "test",
                                        "thing-1",
                                        Payload::from_string("p"),
                                    )
                                    .unwrap();
                                    return Some((
                                        Ok(Message::new(event, NoopAcker, TestCursor(n))),
                                        steps,
                                    ));
                                }
                                Step::Sleep(d) => tokio::time::sleep(d).await,
                                Step::Fail => {
                                    return Some((Err(Error::Store("broken".into())), steps));
                                }
                            }
                        }
                    },
                ))),
            }
        }
    }

    fn quick_config() -> SupervisedConfig {
        SupervisedConfig::new(
            Duration::from_millis(1),
            Duration::from_millis(4),
            2.0,
            Duration::from_secs(60),
        )
        .unwrap()
    }

    async fn next_message(
        stream: &mut SpawnedStream<SupervisedAcker<NoopAcker, TestCursor>, TestCursor>,
    ) -> Message<SupervisedAcker<NoopAcker, TestCursor>, TestCursor> {
        tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn resumes_from_the_last_acknowledged_cursor_after_a_failure() {
        let (reader, opened_with) = ScriptedReader::new(vec![
            Session::Opens(vec![Step::Message(1), Step::Message(2), Step::Fail]),
            Session::Opens(vec![Step::Message(3)]),
        ]);
        let supervised = SupervisedReader::new(reader, quick_config());
        let mut stream = supervised.read(TestSub::default()).await.unwrap();

        let first = next_message(&mut stream).await;
        first.ack().await.unwrap();
        let _second = next_message(&mut stream).await;
        let third = next_message(&mut stream).await;

        assert_eq!(*third.cursor(), TestCursor(3));
        assert_eq!(*opened_with.lock().unwrap(), vec![None, Some(1)]);
    }

    #[tokio::test]
    async fn resumes_from_the_original_subscription_when_nothing_was_acknowledged() {
        let (reader, opened_with) = ScriptedReader::new(vec![
            Session::Opens(vec![Step::Message(1), Step::Fail]),
            Session::Opens(vec![Step::Message(2)]),
        ]);
        let supervised = SupervisedReader::new(reader, quick_config());
        let mut stream = supervised.read(TestSub { after: Some(7) }).await.unwrap();

        let _first = next_message(&mut stream).await;
        let _second = next_message(&mut stream).await;

        assert_eq!(*opened_with.lock().unwrap(), vec![Some(7), Some(7)]);
    }

    #[tokio::test]
    async fn a_nack_does_not_advance_the_resume_cursor() {
        let (reader, opened_with) = ScriptedReader::new(vec![
            Session::Opens(vec![Step::Message(1), Step::Fail]),
            Session::Opens(vec![Step::Message(2)]),
        ]);
        let supervised = SupervisedReader::new(reader, quick_config());
        let mut stream = supervised.read(TestSub::default()).await.unwrap();

        let first = next_message(&mut stream).await;
        first.nack().await.unwrap();
        let _second = next_message(&mut stream).await;

        assert_eq!(*opened_with.lock().unwrap(), vec![None, None]);
    }

    struct RefusingAcker;

    impl Acker for RefusingAcker {
        async fn ack(&self) -> Result<()> {
            Err(Error::Store("ack failed".into()))
        }
        async fn nack(&self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_failed_ack_does_not_advance_the_resume_cursor() {
        let last_acked = Arc::new(Mutex::new(None));
        let acker = SupervisedAcker {
            inner: RefusingAcker,
            cursor: TestCursor(5),
            last_acked: Arc::clone(&last_acked),
        };

        assert!(acker.ack().await.is_err());

        assert_eq!(*last_acked.lock().unwrap(), None);
    }

    #[tokio::test]
    async fn a_refused_reopen_is_retried() {
        let (reader, opened_with) = ScriptedReader::new(vec![
            Session::Opens(vec![Step::Fail]),
            Session::RefusesToOpen,
            Session::Opens(vec![Step::Message(1)]),
        ]);
        let supervised = SupervisedReader::new(reader, quick_config());
        let mut stream = supervised.read(TestSub::default()).await.unwrap();

        let msg = next_message(&mut stream).await;

        assert_eq!(*msg.cursor(), TestCursor(1));
        assert_eq!(opened_with.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn gives_up_after_the_maximum_consecutive_failures() {
        let (reader, opened_with) = ScriptedReader::new(vec![
            Session::RefusesToOpen,
            Session::RefusesToOpen,
            Session::RefusesToOpen,
        ]);
        let supervised =
            SupervisedReader::new(reader, quick_config().with_max_consecutive_failures(2));
        let mut stream = supervised.read(TestSub::default()).await.unwrap();

        let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap();

        assert!(first.unwrap().is_err());
        assert!(second.is_none());
        assert_eq!(opened_with.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn a_healthy_period_resets_the_failure_count() {
        let (reader, _) = ScriptedReader::new(vec![
            Session::Opens(vec![Step::Sleep(Duration::from_millis(60)), Step::Fail]),
            Session::Opens(vec![Step::Sleep(Duration::from_millis(60)), Step::Fail]),
            Session::Opens(vec![Step::Sleep(Duration::from_millis(60)), Step::Fail]),
            Session::Opens(vec![Step::Message(1)]),
        ]);
        let config = SupervisedConfig::new(
            Duration::from_millis(1),
            Duration::from_millis(4),
            2.0,
            Duration::from_millis(30),
        )
        .unwrap()
        .with_max_consecutive_failures(1);
        let supervised = SupervisedReader::new(reader, config);
        let mut stream = supervised.read(TestSub::default()).await.unwrap();

        let msg = next_message(&mut stream).await;

        assert_eq!(*msg.cursor(), TestCursor(1));
    }

    #[tokio::test]
    async fn quick_failures_are_not_a_healthy_period() {
        let (reader, _) = ScriptedReader::new(vec![
            Session::Opens(vec![Step::Fail]),
            Session::Opens(vec![Step::Fail]),
            Session::Opens(vec![Step::Message(1)]),
        ]);
        let config = quick_config().with_max_consecutive_failures(1);
        let supervised = SupervisedReader::new(reader, config);
        let mut stream = supervised.read(TestSub::default()).await.unwrap();

        let result = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap();

        assert!(result.unwrap().is_err());
    }

    #[tokio::test]
    async fn a_finished_stream_ends_the_supervised_stream_by_default() {
        let (reader, opened_with) =
            ScriptedReader::new(vec![Session::Opens(vec![Step::Message(1)])]);
        let supervised = SupervisedReader::new(reader, quick_config());
        let mut stream = supervised.read(TestSub::default()).await.unwrap();

        let _first = next_message(&mut stream).await;
        let end = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap();

        assert!(end.is_none());
        assert_eq!(opened_with.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_finished_stream_is_reopened_when_restart_on_end_is_set() {
        let (reader, opened_with) = ScriptedReader::new(vec![
            Session::Opens(vec![Step::Message(1)]),
            Session::Opens(vec![Step::Message(2)]),
        ]);
        let supervised = SupervisedReader::new(reader, quick_config().with_restart_on_end());
        let mut stream = supervised.read(TestSub::default()).await.unwrap();

        let _first = next_message(&mut stream).await;
        let second = next_message(&mut stream).await;

        assert_eq!(*second.cursor(), TestCursor(2));
        assert_eq!(opened_with.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn cancellation_ends_the_stream_during_backoff() {
        let (reader, _) = ScriptedReader::new(vec![Session::Opens(vec![Step::Fail])]);
        let config = SupervisedConfig::new(
            Duration::from_secs(60),
            Duration::from_secs(60),
            1.0,
            Duration::from_secs(60),
        )
        .unwrap();
        let cancel = CancellationToken::new();
        let supervised = SupervisedReader::new(reader, config).with_cancellation(cancel.clone());
        let mut stream = supervised.read(TestSub::default()).await.unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;
        cancel.cancel();
        let end = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap();

        assert!(end.is_none());
    }

    #[tokio::test]
    async fn cancellation_ends_the_stream_while_waiting_for_messages() {
        let (reader, _) = ScriptedReader::new(vec![]);
        let cancel = CancellationToken::new();
        let supervised =
            SupervisedReader::new(reader, quick_config()).with_cancellation(cancel.clone());
        let mut stream = supervised.read(TestSub::default()).await.unwrap();

        cancel.cancel();
        let end = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap();

        assert!(end.is_none());
    }

    #[test]
    fn backoff_grows_exponentially_up_to_the_ceiling() {
        let config = SupervisedConfig::new(
            Duration::from_millis(100),
            Duration::from_millis(500),
            2.0,
            Duration::from_secs(1),
        )
        .unwrap();

        assert_eq!(config.backoff_after(1), Duration::from_millis(100));
        assert_eq!(config.backoff_after(2), Duration::from_millis(200));
        assert_eq!(config.backoff_after(3), Duration::from_millis(400));
        assert_eq!(config.backoff_after(4), Duration::from_millis(500));
        assert_eq!(config.backoff_after(10_000), Duration::from_millis(500));
    }

    #[test]
    fn config_rejects_a_subunit_multiplier() {
        let result = SupervisedConfig::new(
            Duration::from_millis(1),
            Duration::from_millis(2),
            0.5,
            Duration::from_secs(1),
        );
        assert!(result.is_err());
    }

    #[test]
    fn config_rejects_a_ceiling_below_the_initial_backoff() {
        let result = SupervisedConfig::new(
            Duration::from_millis(10),
            Duration::from_millis(2),
            2.0,
            Duration::from_secs(1),
        );
        assert!(result.is_err());
    }
}
