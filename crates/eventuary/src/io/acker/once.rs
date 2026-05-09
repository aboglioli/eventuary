use std::sync::Arc;

use tokio::sync::Mutex;

use crate::error::Result;
use crate::io::Acker;

#[derive(Debug, Clone)]
pub struct OnceAcker<A: Acker> {
    inner: A,
    done: Arc<Mutex<bool>>,
}

impl<A: Acker> OnceAcker<A> {
    pub fn new(inner: A) -> Self {
        Self {
            inner,
            done: Arc::new(Mutex::new(false)),
        }
    }
}

impl<A: Acker> Acker for OnceAcker<A> {
    async fn ack(&self) -> Result<()> {
        let mut done = self.done.lock().await;
        if *done {
            return Ok(());
        }
        *done = true;
        self.inner.ack().await
    }

    async fn nack(&self) -> Result<()> {
        let mut done = self.done.lock().await;
        if *done {
            return Ok(());
        }
        *done = true;
        self.inner.nack().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Counting {
        acks: Arc<AtomicUsize>,
        nacks: Arc<AtomicUsize>,
    }

    impl Acker for Counting {
        async fn ack(&self) -> Result<()> {
            self.acks.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn nack(&self) -> Result<()> {
            self.nacks.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn ack_is_only_invoked_once() {
        let acks = Arc::new(AtomicUsize::new(0));
        let nacks = Arc::new(AtomicUsize::new(0));
        let acker = OnceAcker::new(Counting {
            acks: Arc::clone(&acks),
            nacks: Arc::clone(&nacks),
        });
        acker.ack().await.unwrap();
        acker.ack().await.unwrap();
        acker.ack().await.unwrap();
        assert_eq!(acks.load(Ordering::SeqCst), 1);
        assert_eq!(nacks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn nack_is_only_invoked_once() {
        let acks = Arc::new(AtomicUsize::new(0));
        let nacks = Arc::new(AtomicUsize::new(0));
        let acker = OnceAcker::new(Counting {
            acks: Arc::clone(&acks),
            nacks: Arc::clone(&nacks),
        });
        acker.nack().await.unwrap();
        acker.nack().await.unwrap();
        assert_eq!(acks.load(Ordering::SeqCst), 0);
        assert_eq!(nacks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn ack_then_nack_only_first_wins() {
        let acks = Arc::new(AtomicUsize::new(0));
        let nacks = Arc::new(AtomicUsize::new(0));
        let acker = OnceAcker::new(Counting {
            acks: Arc::clone(&acks),
            nacks: Arc::clone(&nacks),
        });
        acker.ack().await.unwrap();
        acker.nack().await.unwrap();
        assert_eq!(acks.load(Ordering::SeqCst), 1);
        assert_eq!(nacks.load(Ordering::SeqCst), 0);
    }
}
