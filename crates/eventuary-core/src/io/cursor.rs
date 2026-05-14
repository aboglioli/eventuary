use std::sync::Arc;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum CursorId {
    Global,
    Named(Arc<str>),
}

impl serde::Serialize for CursorId {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            CursorId::Global => s.serialize_str("global"),
            CursorId::Named(id) => s.serialize_str(id),
        }
    }
}

impl<'de> serde::Deserialize<'de> for CursorId {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = String::deserialize(d)?;
        if value == "global" {
            Ok(CursorId::Global)
        } else {
            Ok(CursorId::Named(Arc::from(value)))
        }
    }
}

pub trait Cursor {
    fn id(&self) -> CursorId {
        CursorId::Global
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_id_global_is_default_and_compares_equal() {
        assert_eq!(CursorId::Global, CursorId::Global);
    }

    #[test]
    fn cursor_id_named_is_distinct() {
        let a = CursorId::Named(Arc::from("partition:100:17"));
        let b = CursorId::Named(Arc::from("partition:300:17"));
        assert_ne!(a, b);
    }

    #[test]
    fn cursor_trait_default_is_global() {
        struct SomeCursor;
        impl Cursor for SomeCursor {}
        assert_eq!(SomeCursor.id(), CursorId::Global);
    }

    #[test]
    fn cursor_trait_named_example() {
        struct NamedCursor;
        impl Cursor for NamedCursor {
            fn id(&self) -> CursorId {
                CursorId::Named(Arc::from("partition:100:17"))
            }
        }
        assert_eq!(
            NamedCursor.id(),
            CursorId::Named(Arc::from("partition:100:17"))
        );
    }

    #[test]
    fn cursor_id_global_serializes_as_global_string() {
        let v = serde_json::to_value(CursorId::Global).unwrap();
        assert_eq!(v.as_str(), Some("global"));
    }

    #[test]
    fn cursor_id_named_serializes_as_inner_string() {
        let id = CursorId::Named(Arc::from("partition:100:17"));
        let v = serde_json::to_value(id).unwrap();
        assert_eq!(v.as_str(), Some("partition:100:17"));
    }

    #[test]
    fn cursor_id_global_roundtrips_via_json() {
        let v = serde_json::to_value(CursorId::Global).unwrap();
        let back: CursorId = serde_json::from_value(v).unwrap();
        assert_eq!(back, CursorId::Global);
    }

    #[test]
    fn cursor_id_named_roundtrips_via_json() {
        let id = CursorId::Named(Arc::from("partition:4:1"));
        let v = serde_json::to_value(id.clone()).unwrap();
        let back: CursorId = serde_json::from_value(v).unwrap();
        assert_eq!(back, id);
    }
}
