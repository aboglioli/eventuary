use std::fmt;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::partition::Partition;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct CursorId(Arc<str>);

impl CursorId {
    pub fn new(value: impl Into<Arc<str>>) -> Result<Self> {
        let value: Arc<str> = value.into();
        if value.is_empty() || value.len() > 128 {
            return Err(Error::Config(format!(
                "invalid cursor id: {:?}",
                value.as_ref()
            )));
        }
        if !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.' || c == ':')
        {
            return Err(Error::Config(format!(
                "invalid cursor id: {:?}",
                value.as_ref()
            )));
        }
        Ok(Self(value))
    }

    pub fn global() -> Self {
        Self(Arc::from("global"))
    }

    pub fn partition(partition: Partition) -> Self {
        Self(Arc::from(format!(
            "partition:{count}:{id}",
            count = partition.count(),
            id = partition.id(),
        )))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn prefixed(&self, prefix: impl AsRef<str>) -> Result<Self> {
        Self::new(format!("{}:{}", prefix.as_ref(), self.as_str()))
    }
}

impl fmt::Display for CursorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl serde::Serialize for CursorId {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.0)
    }
}

impl<'de> serde::Deserialize<'de> for CursorId {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let value = String::deserialize(d)?;
        CursorId::new(Arc::from(value)).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct CursorOrder(Arc<[u8]>);

impl CursorOrder {
    pub fn min() -> Self {
        Self(Arc::from(&[][..]))
    }

    pub fn from_bytes(bytes: impl Into<Arc<[u8]>>) -> Self {
        Self(bytes.into())
    }

    pub fn from_u64(value: u64) -> Self {
        Self(Arc::from(value.to_be_bytes().as_slice()))
    }

    pub fn from_i64(value: i64) -> Self {
        let mapped = (value as u64) ^ (1u64 << 63);
        Self::from_u64(mapped)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl serde::Serialize for CursorOrder {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD;
        s.serialize_str(&STANDARD.encode(&self.0))
    }
}

impl<'de> serde::Deserialize<'de> for CursorOrder {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD;
        let raw = String::deserialize(d)?;
        let bytes = STANDARD
            .decode(raw.as_bytes())
            .map_err(serde::de::Error::custom)?;
        Ok(Self(Arc::from(bytes.as_slice())))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct CursorKind(Arc<str>);

impl CursorKind {
    pub fn new(value: impl Into<Arc<str>>) -> Result<Self> {
        let value: Arc<str> = value.into();
        if value.is_empty() || value.len() > 128 {
            return Err(Error::Config(format!(
                "invalid cursor kind: {:?}",
                value.as_ref()
            )));
        }
        if !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.' || c == ':')
        {
            return Err(Error::Config(format!(
                "invalid cursor kind: {:?}",
                value.as_ref()
            )));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CursorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for CursorKind {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for CursorKind {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let value = String::deserialize(d)?;
        CursorKind::new(Arc::from(value)).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct EncodedCursor {
    id: CursorId,
    kind: CursorKind,
    order: CursorOrder,
    payload: Arc<str>,
}

#[derive(Serialize, Deserialize)]
struct EncodedCursorRepr {
    id: CursorId,
    kind: CursorKind,
    order: CursorOrder,
    payload: String,
}

impl Serialize for EncodedCursor {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        let repr = EncodedCursorRepr {
            id: self.id.clone(),
            kind: self.kind.clone(),
            order: self.order.clone(),
            payload: self.payload.as_ref().to_owned(),
        };
        repr.serialize(s)
    }
}

impl<'de> Deserialize<'de> for EncodedCursor {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let repr = EncodedCursorRepr::deserialize(d)?;
        Ok(Self {
            id: repr.id,
            kind: repr.kind,
            order: repr.order,
            payload: Arc::from(repr.payload),
        })
    }
}

impl EncodedCursor {
    pub fn new(
        id: CursorId,
        kind: CursorKind,
        order: CursorOrder,
        payload: impl Into<Arc<str>>,
    ) -> Result<Self> {
        let payload: Arc<str> = payload.into();
        if payload.is_empty() {
            return Err(Error::InvalidCursor(
                "encoded cursor payload must not be empty".to_owned(),
            ));
        }

        use base64::Engine;
        use base64::engine::general_purpose::STANDARD;
        STANDARD.decode(payload.as_bytes()).map_err(|e| {
            Error::InvalidCursor(format!("encoded cursor payload is not base64: {e}"))
        })?;

        Ok(Self {
            id,
            kind,
            order,
            payload,
        })
    }

    pub fn from_bytes(
        id: CursorId,
        kind: CursorKind,
        order: CursorOrder,
        bytes: impl AsRef<[u8]>,
    ) -> Self {
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD;
        Self {
            id,
            kind,
            order,
            payload: Arc::from(STANDARD.encode(bytes.as_ref())),
        }
    }

    pub fn from_json<T: Serialize>(
        id: CursorId,
        kind: CursorKind,
        order: CursorOrder,
        value: &T,
    ) -> Result<Self> {
        let bytes = serde_json::to_vec(value)
            .map_err(|e| Error::Serialization(format!("encode cursor json: {e}")))?;
        Ok(Self::from_bytes(id, kind, order, bytes))
    }

    pub fn decode_bytes(&self) -> Result<Vec<u8>> {
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD;
        STANDARD
            .decode(self.payload.as_bytes())
            .map_err(|e| Error::InvalidCursor(format!("decode cursor payload: {e}")))
    }

    pub fn decode_json<T: DeserializeOwned>(&self, expected_kind: &CursorKind) -> Result<T> {
        if &self.kind != expected_kind {
            return Err(Error::InvalidCursor(format!(
                "expected cursor kind `{}`, got `{}`",
                expected_kind.as_str(),
                self.kind.as_str()
            )));
        }
        serde_json::from_slice(&self.decode_bytes()?)
            .map_err(|e| Error::InvalidCursor(format!("decode cursor json: {e}")))
    }

    pub fn id_ref(&self) -> &CursorId {
        &self.id
    }

    pub fn kind(&self) -> &CursorKind {
        &self.kind
    }

    pub fn order(&self) -> &CursorOrder {
        &self.order
    }

    pub fn payload(&self) -> &str {
        &self.payload
    }
}

impl PartialOrd for EncodedCursor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EncodedCursor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id).then(self.order.cmp(&other.order))
    }
}

impl Cursor for EncodedCursor {
    fn id(&self) -> CursorId {
        self.id.clone()
    }

    fn order_key(&self) -> CursorOrder {
        self.order.clone()
    }
}

pub trait CursorCodec<C>: Clone + Send + Sync + 'static {
    fn encode(&self, cursor: &C) -> Result<EncodedCursor>;

    fn decode(&self, cursor: &EncodedCursor) -> Result<C>;
}

#[derive(Debug)]
pub struct JsonCursorCodec<C> {
    kind: CursorKind,
    _cursor: std::marker::PhantomData<fn() -> C>,
}

impl<C> Clone for JsonCursorCodec<C> {
    fn clone(&self) -> Self {
        Self {
            kind: self.kind.clone(),
            _cursor: std::marker::PhantomData,
        }
    }
}

impl<C> JsonCursorCodec<C> {
    pub fn new(kind: impl Into<Arc<str>>) -> Result<Self> {
        Ok(Self {
            kind: CursorKind::new(kind)?,
            _cursor: std::marker::PhantomData,
        })
    }

    pub fn kind(&self) -> &CursorKind {
        &self.kind
    }
}

impl<C> CursorCodec<C> for JsonCursorCodec<C>
where
    C: Cursor + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn encode(&self, cursor: &C) -> Result<EncodedCursor> {
        EncodedCursor::from_json(cursor.id(), self.kind.clone(), cursor.order_key(), cursor)
    }

    fn decode(&self, cursor: &EncodedCursor) -> Result<C> {
        cursor.decode_json(&self.kind)
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NoCursor;

pub trait Cursor {
    fn id(&self) -> CursorId {
        CursorId::global()
    }

    fn order_key(&self) -> CursorOrder {
        CursorOrder::min()
    }
}

impl Cursor for NoCursor {}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;

    fn partition(count: u32, id: u32) -> Partition {
        Partition::new(id, NonZeroU32::new(count).unwrap()).unwrap()
    }

    #[test]
    fn global_returns_static_global_id() {
        assert_eq!(CursorId::global().as_str(), "global");
        assert_eq!(CursorId::global(), CursorId::global());
    }

    #[test]
    fn partition_constructs_stable_format() {
        let id = CursorId::partition(partition(100, 17));
        assert_eq!(id.as_str(), "partition:100:17");
    }

    #[test]
    fn new_validates_and_stores_value() {
        let id = CursorId::new("custom.stream").unwrap();
        assert_eq!(id.as_str(), "custom.stream");
    }

    #[test]
    fn new_rejects_empty() {
        assert!(CursorId::new("").is_err());
    }

    #[test]
    fn new_rejects_too_long() {
        let long = "a".repeat(129);
        assert!(CursorId::new(long).is_err());
    }

    #[test]
    fn new_rejects_invalid_chars() {
        assert!(CursorId::new("bad space").is_err());
        assert!(CursorId::new("bad/char").is_err());
    }

    #[test]
    fn new_accepts_valid_chars() {
        assert!(CursorId::new("valid-name_01.v2:tag").is_ok());
    }

    #[test]
    fn equality_by_value() {
        let a = CursorId::new("test").unwrap();
        let b = CursorId::new("test").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn distinct_values_differ() {
        let a = CursorId::new("a").unwrap();
        let b = CursorId::new("b").unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn cursor_id_orders_lexically() {
        let a = CursorId::new("a").unwrap();
        let b = CursorId::new("b").unwrap();
        assert!(a < b);
    }

    #[test]
    fn prefixed_adds_stable_prefix() {
        let id = CursorId::global().prefixed("left").unwrap();
        assert_eq!(id.as_str(), "left:global");
    }

    #[test]
    fn prefixed_rejects_invalid_prefix() {
        let err = CursorId::global().prefixed("bad prefix").unwrap_err();
        assert!(err.to_string().contains("invalid cursor id"));
    }

    #[test]
    fn cursor_order_from_i64_preserves_signed_ordering() {
        let lo = CursorOrder::from_i64(i64::MIN);
        let zero = CursorOrder::from_i64(0);
        let hi = CursorOrder::from_i64(i64::MAX);
        assert!(lo < zero);
        assert!(zero < hi);
        assert!(CursorOrder::from_i64(-1) < CursorOrder::from_i64(1));
        assert!(CursorOrder::from_i64(9) < CursorOrder::from_i64(10));
    }

    #[test]
    fn cursor_order_from_u64_preserves_ordering() {
        assert!(CursorOrder::from_u64(9) < CursorOrder::from_u64(10));
    }

    #[test]
    fn cursor_order_roundtrips_via_json() {
        let v = CursorOrder::from_i64(-42);
        let s = serde_json::to_string(&v).unwrap();
        let back: CursorOrder = serde_json::from_str(&s).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn cursor_order_min_is_smallest() {
        assert!(CursorOrder::min() < CursorOrder::from_u64(0));
    }

    #[test]
    fn cursor_kind_validates() {
        assert!(CursorKind::new("eventuary.postgres.cursor.v1").is_ok());
        assert!(CursorKind::new("").is_err());
        assert!(CursorKind::new("bad kind").is_err());
    }

    #[test]
    fn encoded_cursor_roundtrips_json() {
        #[derive(Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
        struct TestCursor {
            sequence: i64,
        }

        let kind = CursorKind::new("eventuary.test.cursor.v1").unwrap();
        let cursor = EncodedCursor::from_json(
            CursorId::global(),
            kind.clone(),
            CursorOrder::from_i64(42),
            &TestCursor { sequence: 42 },
        )
        .unwrap();

        assert_eq!(cursor.id_ref(), &CursorId::global());
        assert_eq!(cursor.kind(), &kind);
        assert_eq!(cursor.order(), &CursorOrder::from_i64(42));
        let decoded: TestCursor = cursor.decode_json(&kind).unwrap();
        assert_eq!(decoded, TestCursor { sequence: 42 });
    }

    #[test]
    fn encoded_cursor_rejects_wrong_kind() {
        let kind = CursorKind::new("eventuary.test.cursor.v1").unwrap();
        let other = CursorKind::new("eventuary.other.cursor.v1").unwrap();
        let cursor =
            EncodedCursor::from_json(CursorId::global(), kind, CursorOrder::from_i64(0), &42_i64)
                .unwrap();

        let err = cursor.decode_json::<i64>(&other).unwrap_err();
        assert!(err.to_string().contains("expected cursor kind"));
    }

    #[test]
    fn encoded_cursor_orders_by_id_then_order() {
        let kind = CursorKind::new("eventuary.test.cursor.v1").unwrap();
        let a9 = EncodedCursor::from_bytes(
            CursorId::new("a").unwrap(),
            kind.clone(),
            CursorOrder::from_i64(9),
            b"x",
        );
        let a10 = EncodedCursor::from_bytes(
            CursorId::new("a").unwrap(),
            kind.clone(),
            CursorOrder::from_i64(10),
            b"x",
        );
        let b0 = EncodedCursor::from_bytes(
            CursorId::new("b").unwrap(),
            kind,
            CursorOrder::from_i64(0),
            b"x",
        );

        assert!(a9 < a10);
        assert!(a10 < b0);
    }

    #[test]
    fn encoded_cursor_new_validates_payload() {
        let kind = CursorKind::new("eventuary.test.cursor.v1").unwrap();
        let err = EncodedCursor::new(
            CursorId::global(),
            kind.clone(),
            CursorOrder::from_i64(0),
            "",
        )
        .unwrap_err();
        assert!(err.to_string().contains("must not be empty"));

        let err = EncodedCursor::new(
            CursorId::global(),
            kind.clone(),
            CursorOrder::from_i64(0),
            "not-base64-$$",
        )
        .unwrap_err();
        assert!(err.to_string().contains("not base64"));

        let ok =
            EncodedCursor::from_bytes(CursorId::global(), kind, CursorOrder::from_i64(0), b"x");
        let rebuilt = EncodedCursor::new(
            ok.id_ref().clone(),
            ok.kind().clone(),
            ok.order().clone(),
            ok.payload().to_owned(),
        )
        .unwrap();
        assert_eq!(rebuilt, ok);
    }

    #[test]
    fn encoded_cursor_implements_cursor_trait() {
        let kind = CursorKind::new("eventuary.test.cursor.v1").unwrap();
        let cursor = EncodedCursor::from_bytes(
            CursorId::new("custom").unwrap(),
            kind,
            CursorOrder::from_u64(7),
            b"p",
        );
        assert_eq!(<EncodedCursor as Cursor>::id(&cursor).as_str(), "custom");
        assert_eq!(
            <EncodedCursor as Cursor>::order_key(&cursor),
            CursorOrder::from_u64(7)
        );
    }

    #[test]
    fn json_cursor_codec_roundtrips() {
        #[derive(
            Debug, Clone, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize,
        )]
        struct TestCursor(i64);

        impl Cursor for TestCursor {
            fn order_key(&self) -> CursorOrder {
                CursorOrder::from_i64(self.0)
            }
        }

        let codec = JsonCursorCodec::<TestCursor>::new("eventuary.test.cursor.v1").unwrap();
        let encoded = codec.encode(&TestCursor(7)).unwrap();
        assert_eq!(encoded.id_ref(), &CursorId::global());
        assert_eq!(encoded.kind(), codec.kind());
        assert_eq!(encoded.order(), &CursorOrder::from_i64(7));

        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, TestCursor(7));
    }

    #[test]
    fn json_cursor_codec_preserves_typed_ordering_under_erasure() {
        #[derive(
            Debug, Clone, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize,
        )]
        struct TestCursor(i64);

        impl Cursor for TestCursor {
            fn order_key(&self) -> CursorOrder {
                CursorOrder::from_i64(self.0)
            }
        }

        let codec = JsonCursorCodec::<TestCursor>::new("eventuary.test.cursor.v1").unwrap();
        for (a, b) in [(-5_i64, 0), (0, 1), (9, 10), (i64::MIN, i64::MAX)] {
            let ea = codec.encode(&TestCursor(a)).unwrap();
            let eb = codec.encode(&TestCursor(b)).unwrap();
            assert_eq!(
                TestCursor(a).cmp(&TestCursor(b)),
                ea.cmp(&eb),
                "a={a} b={b}"
            );
        }
    }

    #[test]
    fn encoded_cursor_roundtrips_via_serde() {
        let kind = CursorKind::new("eventuary.test.cursor.v1").unwrap();
        let cursor =
            EncodedCursor::from_json(CursorId::global(), kind, CursorOrder::from_i64(42), &42_i64)
                .unwrap();

        let s = serde_json::to_string(&cursor).unwrap();
        let back: EncodedCursor = serde_json::from_str(&s).unwrap();
        assert_eq!(cursor, back);
    }

    #[test]
    fn cursor_trait_default_is_global() {
        struct SomeCursor;
        impl Cursor for SomeCursor {}
        assert_eq!(SomeCursor.id(), CursorId::global());
    }

    #[test]
    fn cursor_trait_named_example() {
        struct NamedCursor;
        impl Cursor for NamedCursor {
            fn id(&self) -> CursorId {
                CursorId::new("partition:100:17").unwrap()
            }
        }
        assert_eq!(NamedCursor.id(), CursorId::partition(partition(100, 17)));
    }

    #[test]
    fn cursor_trait_default_order_key_is_min() {
        struct Probe;
        impl Cursor for Probe {}
        assert_eq!(Probe.order_key(), CursorOrder::min());
    }

    #[test]
    fn cursor_trait_named_example_supports_custom_order() {
        struct OrderedCursor(i64);
        impl Cursor for OrderedCursor {
            fn order_key(&self) -> CursorOrder {
                CursorOrder::from_i64(self.0)
            }
        }
        assert!(OrderedCursor(9).order_key() < OrderedCursor(10).order_key());
    }

    #[test]
    fn cursor_trait_does_not_require_ord() {
        struct UnorderedCursor;

        impl Cursor for UnorderedCursor {}

        assert_eq!(UnorderedCursor.id(), CursorId::global());
        assert_eq!(UnorderedCursor.order_key(), CursorOrder::min());
    }

    #[test]
    fn serializes_as_plain_string() {
        let id = CursorId::global();
        let v = serde_json::to_value(id).unwrap();
        assert_eq!(v.as_str(), Some("global"));

        let id = CursorId::partition(partition(4, 1));
        let v = serde_json::to_value(id).unwrap();
        assert_eq!(v.as_str(), Some("partition:4:1"));
    }

    #[test]
    fn roundtrips_via_json() {
        let id = CursorId::global();
        let v = serde_json::to_value(id.clone()).unwrap();
        let back: CursorId = serde_json::from_value(v).unwrap();
        assert_eq!(back, id);

        let id = CursorId::partition(partition(4, 2));
        let v = serde_json::to_value(id.clone()).unwrap();
        let back: CursorId = serde_json::from_value(v).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn display_output_matches_as_str() {
        let id = CursorId::global();
        assert_eq!(id.to_string(), "global");

        let id = CursorId::partition(partition(4, 1));
        assert_eq!(id.to_string(), "partition:4:1");
    }

    #[test]
    fn no_cursor_uses_global_cursor_id() {
        assert_eq!(NoCursor.id(), CursorId::global());
    }
}
