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
pub use filter::{ArcFilter, BoxFilter, Filter, FilterExt};
pub use handler::{ArcHandler, BoxHandler, DynHandler, Handler, HandlerExt};
pub use reader::{ArcReader, BoxReader, BoxStream, DynReader, Reader, ReaderExt};
pub use writer::{ArcWriter, BoxWriter, DynWriter, Writer, WriterExt};

pub use cursor::{Cursor, CursorId, NoCursor};

pub use consumer_group_id::ConsumerGroupId;
pub use duplex::Duplex;
pub use message::Message;
pub use owner_id::OwnerId;
pub use position::{PartitionableSubscription, StartFrom, StartableSubscription, StopAt};
pub use stream_id::StreamId;

#[cfg(test)]
mod tests {
    #[test]
    fn base_trait_companions_are_available_at_io_level() {
        #[derive(Debug, Clone, PartialEq, Eq)]
        struct UserUpdated {
            user_id: String,
        }

        fn assert_type<T>() {}

        assert_type::<crate::io::BoxWriter>();
        assert_type::<crate::io::ArcWriter>();
        assert_type::<crate::io::BoxWriter<UserUpdated>>();
        assert_type::<crate::io::ArcWriter<UserUpdated>>();
        assert_type::<crate::io::BoxHandler>();
        assert_type::<crate::io::ArcHandler>();
        assert_type::<crate::io::BoxHandler<UserUpdated>>();
        assert_type::<crate::io::ArcHandler<UserUpdated>>();
        assert_type::<crate::io::BoxFilter>();
        assert_type::<crate::io::ArcFilter>();
        assert_type::<crate::io::BoxFilter<UserUpdated>>();
        assert_type::<crate::io::ArcFilter<UserUpdated>>();
    }

    #[test]
    fn wrapper_types_are_available_at_submodule_paths() {
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
        assert_type::<crate::io::writer::FilteredWriter<(), crate::io::filter::AllFilter>>();
        assert_type::<crate::io::writer::RetryWriter<()>>();
        assert_type::<crate::io::writer::TimeoutWriter<()>>();
        assert_type::<crate::io::writer::InspectWriter<(), ()>>();
        assert_type::<crate::io::writer::BatchWriter>();
        assert_type::<crate::io::writer::FlatMapWriter<(), fn(&crate::Event) -> Vec<crate::Event>>>(
        );
        assert_type::<
            crate::io::writer::TryFlatMapWriter<
                (),
                fn(&crate::Event) -> crate::Result<Vec<crate::Event>>,
            >,
        >();
        assert_type::<
            crate::io::reader::DecodeReader<
                (),
                crate::PayloadEventCodec<crate::JsonPayloadCodec>,
                crate::Payload,
            >,
        >();
        assert_type::<crate::io::reader::OutcomeRouterReader<()>>();
        assert_type::<crate::io::reader::NackDisposition>();
        assert_type::<crate::io::handler::TimeoutHandler<()>>();
        assert_type::<crate::io::handler::InspectHandler<(), ()>>();
        assert_type::<crate::io::handler::RateLimitHandler<()>>();
        assert_type::<crate::io::handler::FilteredHandler<(), crate::io::filter::AllFilter>>();
    }

    #[test]
    fn typed_payload_wrappers_are_available_at_submodule_paths() {
        #[derive(Debug, Clone, PartialEq, Eq)]
        struct UserUpdated {
            user_id: String,
        }

        fn assert_type<T>() {}

        assert_type::<crate::io::writer::FilteredWriter<(), crate::io::filter::AllFilter>>();
        assert_type::<crate::io::writer::RetryWriter<()>>();
        assert_type::<crate::io::writer::TimeoutWriter<()>>();
        assert_type::<crate::io::writer::InspectWriter<(), (), UserUpdated>>();
        assert_type::<crate::io::writer::BatchWriter<UserUpdated>>();
        assert_type::<crate::io::writer::FanoutWriter<UserUpdated>>();
        assert_type::<
            crate::io::writer::MapWriter<
                (),
                fn(&crate::Event<UserUpdated>) -> crate::Event<UserUpdated>,
                UserUpdated,
                UserUpdated,
            >,
        >();
        assert_type::<crate::io::handler::TimeoutHandler<()>>();
        assert_type::<crate::io::handler::InspectHandler<(), (), UserUpdated>>();
        assert_type::<crate::io::handler::RateLimitHandler<()>>();
        assert_type::<crate::io::handler::FilteredHandler<(), crate::io::filter::AllFilter>>();
    }
}
