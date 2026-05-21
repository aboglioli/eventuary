pub use futures::future::BoxFuture;

pub mod acker;
pub mod consumer;
pub mod consumer_group_id;
pub mod cursor;
pub mod duplex;
pub mod filter;
pub mod handler;
pub mod message;
pub mod position;
pub mod reader;
pub mod stream;
pub mod stream_id;
pub mod writer;

pub use acker::{Acker, AckerExt, ArcAcker, BoxAcker, DynAcker};
pub use consumer_group_id::ConsumerGroupId;
pub use cursor::{
    Cursor, CursorCodec, CursorId, CursorKind, CursorOrder, EncodedCursor, JsonCursorCodec,
    NoCursor,
};
pub use duplex::Duplex;
pub use filter::{ArcFilter, BoxFilter, Filter, FilterExt};
pub use handler::{ArcHandler, BoxHandler, DynHandler, Handler, HandlerExt};
pub use message::Message;
pub use position::{StartFrom, StartableSubscription, StopAt};
pub use reader::{ArcReader, BoxReader, BoxStream, DynReader, Reader, ReaderExt};
pub use stream_id::StreamId;
pub use writer::{
    ArcWriter, BoxWriter, DynWriter, FanoutWriter, MapWriter, TryMapWriter, Writer, WriterExt,
};

#[cfg(test)]
mod tests {
    #[test]
    fn flow_wrapper_reexports_are_available() {
        fn assert_type<T>() {}

        assert_type::<crate::io::writer::MapWriter<(), fn(&crate::Event) -> crate::Event>>();
        assert_type::<
            crate::io::writer::TryMapWriter<(), fn(&crate::Event) -> crate::Result<crate::Event>>,
        >();
        assert_type::<crate::io::writer::FanoutWriter>();
        assert_type::<crate::io::reader::OutcomeRouterReader<()>>();
        assert_type::<crate::io::reader::NackDisposition>();
        assert_type::<crate::io::MapWriter<(), fn(&crate::Event) -> crate::Event>>();
        assert_type::<crate::io::TryMapWriter<(), fn(&crate::Event) -> crate::Result<crate::Event>>>(
        );
        assert_type::<crate::io::FanoutWriter>();
    }
}
