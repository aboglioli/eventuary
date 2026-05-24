pub use futures::future::BoxFuture;

pub mod acker;
pub mod consumer;
pub mod consumer_group_id;
pub mod cursor;
pub mod duplex;
pub mod filter;
pub mod handler;
pub mod message;
pub mod owner_id;
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
pub use handler::{
    ArcHandler, BoxHandler, DynHandler, Handler, HandlerExt, HandlerRateLimit, InspectHandler,
    InspectHandlerHooks, RateLimitHandler, TimeoutHandler,
};
pub use message::Message;
pub use owner_id::OwnerId;
pub use position::{PartitionableSubscription, StartFrom, StartableSubscription, StopAt};
pub use reader::{
    ArcReader, BoxReader, BoxStream, DecodeErrorDisposition, DecodeReader, DynReader, Reader,
    ReaderExt, ReaderTypedExt,
};
pub use stream_id::StreamId;
pub use writer::{
    ArcWriter, BatchWriter, BatchWriterConfig, BoxWriter, DynWriter, EncodeWriter, FanoutWriter,
    FilteredWriter, FlatMapWriter, InspectWriter, InspectWriterHooks, MapWriter, RetryWriter,
    RetryWriterConfig, TimeoutWriter, TryFlatMapWriter, TryMapWriter, Writer, WriterExt,
    WriterTypedExt,
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
        assert_type::<
            crate::io::writer::EncodeWriter<
                (),
                crate::PayloadEventCodec<crate::JsonPayloadCodec>,
                crate::Payload,
            >,
        >();
        assert_type::<crate::io::writer::FanoutWriter>();
        assert_type::<crate::io::reader::OutcomeRouterReader<()>>();
        assert_type::<crate::io::reader::NackDisposition>();
        assert_type::<crate::io::MapWriter<(), fn(&crate::Event) -> crate::Event>>();
        assert_type::<crate::io::TryMapWriter<(), fn(&crate::Event) -> crate::Result<crate::Event>>>(
        );
        assert_type::<
            crate::io::EncodeWriter<
                (),
                crate::PayloadEventCodec<crate::JsonPayloadCodec>,
                crate::Payload,
            >,
        >();
        assert_type::<
            crate::io::DecodeReader<
                (),
                crate::PayloadEventCodec<crate::JsonPayloadCodec>,
                crate::Payload,
            >,
        >();
        assert_type::<crate::io::FanoutWriter>();
    }

    #[test]
    fn writer_and_handler_wrapper_reexports_are_available() {
        fn assert_type<T>() {}

        assert_type::<crate::io::FilteredWriter<(), crate::io::filter::AllFilter>>();
        assert_type::<crate::io::RetryWriter<()>>();
        assert_type::<crate::io::TimeoutWriter<()>>();
        assert_type::<crate::io::InspectWriter<(), ()>>();
        assert_type::<crate::io::BatchWriter>();
        assert_type::<crate::io::FlatMapWriter<(), fn(&crate::Event) -> Vec<crate::Event>>>();
        assert_type::<
            crate::io::TryFlatMapWriter<(), fn(&crate::Event) -> crate::Result<Vec<crate::Event>>>,
        >();
        assert_type::<crate::io::TimeoutHandler<()>>();
        assert_type::<crate::io::InspectHandler<(), ()>>();
        assert_type::<crate::io::RateLimitHandler<()>>();
    }

    #[test]
    fn typed_payload_wrapper_reexports_are_available() {
        #[derive(Debug, Clone, PartialEq, Eq)]
        struct UserUpdated {
            user_id: String,
        }

        fn assert_type<T>() {}

        assert_type::<crate::io::BoxWriter<UserUpdated>>();
        assert_type::<crate::io::ArcWriter<UserUpdated>>();
        assert_type::<crate::io::BoxHandler<UserUpdated>>();
        assert_type::<crate::io::ArcHandler<UserUpdated>>();
        assert_type::<crate::io::BoxFilter<UserUpdated>>();
        assert_type::<crate::io::ArcFilter<UserUpdated>>();

        assert_type::<crate::io::FilteredWriter<(), crate::io::filter::AllFilter>>();
        assert_type::<crate::io::RetryWriter<()>>();
        assert_type::<crate::io::TimeoutWriter<()>>();
        assert_type::<crate::io::InspectWriter<(), (), UserUpdated>>();
        assert_type::<crate::io::BatchWriter<UserUpdated>>();
        assert_type::<crate::io::FanoutWriter<UserUpdated>>();
        assert_type::<
            crate::io::MapWriter<
                (),
                fn(&crate::Event<UserUpdated>) -> crate::Event<UserUpdated>,
                UserUpdated,
                UserUpdated,
            >,
        >();

        assert_type::<crate::io::TimeoutHandler<()>>();
        assert_type::<crate::io::InspectHandler<(), (), UserUpdated>>();
        assert_type::<crate::io::RateLimitHandler<()>>();
        assert_type::<crate::io::handler::FilteredHandler<(), crate::io::filter::AllFilter>>();
    }
}
