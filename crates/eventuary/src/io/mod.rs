#![allow(clippy::needless_maybe_sized)]

pub use futures::future::BoxFuture;

pub mod acker;
pub mod consumers;
pub mod filters;
mod handler;
mod message;
mod reader;
mod writer;

pub use acker::{Acker, AckerExt, ArcAcker, BoxAcker, DynAcker};
pub use consumers::{
    BackgroundConsumer, ConsumerHandle, DeadLetterWriter, DefaultRetryPolicy, RetryAction,
    RetryConfig, RetryHandler, RetryPolicy, backoff_delay,
};
pub use handler::{
    ArcFilter, ArcHandler, BoxFilter, BoxHandler, DynHandler, Filter, FilterExt, FilteredHandler,
    Handler, HandlerExt,
};
pub use message::Message;
pub use reader::{ArcReader, BoxReader, BoxStream, DynReader, Reader, ReaderExt};
pub use writer::{ArcWriter, BoxWriter, DynWriter, Writer, WriterExt};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn box_future_alias_accepts_send_boxed_future() {
        let future: BoxFuture<'static, crate::Result<()>> = Box::pin(async { Ok(()) });
        drop(future);
    }
}
