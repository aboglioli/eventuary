use eventuary_core::{Error, Event, EventId, Result, SerializedEvent};

pub(crate) const MAX_ENTRIES: usize = 10;
pub(crate) const MAX_PAYLOAD_BYTES: usize = 256 * 1024;

pub(crate) fn serialize_body(event: &Event) -> Result<String> {
    let body = SerializedEvent::from_event(event)?.to_json_string()?;
    if body.len() > MAX_PAYLOAD_BYTES {
        return Err(Error::InvalidPayload(format!(
            "event body {} bytes exceeds 256 KB",
            body.len()
        )));
    }
    Ok(body)
}

pub(crate) struct Batcher<E> {
    entries: Vec<E>,
    event_ids: Vec<EventId>,
    bytes: usize,
}

impl<E> Batcher<E> {
    pub(crate) fn new() -> Self {
        Self {
            entries: Vec::new(),
            event_ids: Vec::new(),
            bytes: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn would_exceed(&self, next_body: usize) -> bool {
        self.entries.len() == MAX_ENTRIES || self.bytes + next_body > MAX_PAYLOAD_BYTES
    }

    pub(crate) fn push(&mut self, entry: E, event_id: EventId, body_len: usize) {
        self.entries.push(entry);
        self.event_ids.push(event_id);
        self.bytes += body_len;
    }

    pub(crate) fn take(&mut self) -> (Vec<E>, Vec<EventId>) {
        self.bytes = 0;
        (
            std::mem::take(&mut self.entries),
            std::mem::take(&mut self.event_ids),
        )
    }
}

pub(crate) struct FailedEntry<'a> {
    pub(crate) id: &'a str,
    pub(crate) code: &'a str,
    pub(crate) sender_fault: bool,
    pub(crate) message: Option<&'a str>,
}

pub(crate) fn failure_report(
    operation: &str,
    failed: &[FailedEntry<'_>],
    event_ids: &[EventId],
) -> Error {
    let details: Vec<String> = failed
        .iter()
        .map(|f| {
            let event =
                f.id.parse::<usize>()
                    .ok()
                    .and_then(|i| event_ids.get(i))
                    .map(EventId::to_string)
                    .unwrap_or_else(|| f.id.to_owned());
            format!(
                "event={} code={} sender_fault={} message={}",
                event,
                f.code,
                f.sender_fault,
                f.message.unwrap_or("")
            )
        })
        .collect();
    Error::Store(format!(
        "{operation} had {} failed entries: {}",
        failed.len(),
        details.join("; ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventuary_core::Payload;

    fn event(key: &str) -> Event {
        Event::create(
            "acme",
            "/orders",
            "order.placed",
            key,
            Payload::from_string("v"),
        )
        .expect("valid event")
    }

    fn batch_sizes(event_count: usize, body_len: usize) -> Vec<usize> {
        let mut batcher: Batcher<()> = Batcher::new();
        let mut sizes = Vec::new();
        for _ in 0..event_count {
            if batcher.would_exceed(body_len) {
                sizes.push(batcher.len());
                batcher.take();
            }
            batcher.push((), event("k").id(), body_len);
        }
        if !batcher.is_empty() {
            sizes.push(batcher.len());
        }
        sizes
    }

    #[test]
    fn flushes_only_once_the_entry_limit_is_reached() {
        let mut batcher: Batcher<()> = Batcher::new();
        for _ in 0..MAX_ENTRIES - 1 {
            batcher.push((), event("k").id(), 1);
        }
        assert!(!batcher.would_exceed(1));
        batcher.push((), event("k").id(), 1);
        assert!(batcher.would_exceed(1));
    }

    #[test]
    fn flushes_before_the_payload_limit_is_crossed() {
        let mut batcher: Batcher<()> = Batcher::new();
        batcher.push((), event("k").id(), MAX_PAYLOAD_BYTES - 1);
        assert!(!batcher.would_exceed(1));
        let mut full: Batcher<()> = Batcher::new();
        full.push((), event("k").id(), MAX_PAYLOAD_BYTES);
        assert!(full.would_exceed(1));
    }

    #[test]
    fn no_batch_ever_exceeds_the_entry_limit() {
        for count in [0, 1, 9, 10, 11, 25, 100, 1001] {
            let sizes = batch_sizes(count, 16);
            assert!(
                sizes.iter().all(|n| *n <= MAX_ENTRIES),
                "count {count} produced {sizes:?}"
            );
            assert_eq!(sizes.iter().sum::<usize>(), count);
            assert!(sizes.iter().all(|n| *n > 0));
        }
    }

    #[test]
    fn exactly_ten_events_form_one_batch() {
        assert_eq!(batch_sizes(10, 16), vec![10]);
        assert_eq!(batch_sizes(11, 16), vec![10, 1]);
        assert_eq!(batch_sizes(25, 16), vec![10, 10, 5]);
    }

    #[test]
    fn oversized_bodies_split_before_the_entry_limit() {
        assert_eq!(batch_sizes(4, MAX_PAYLOAD_BYTES / 2), vec![2, 2]);
    }

    #[test]
    fn take_resets_the_running_byte_count() {
        let mut batcher: Batcher<()> = Batcher::new();
        batcher.push((), event("k").id(), MAX_PAYLOAD_BYTES);
        let (entries, ids) = batcher.take();
        assert_eq!(entries.len(), 1);
        assert_eq!(ids.len(), 1);
        assert!(batcher.is_empty());
        assert!(!batcher.would_exceed(1));
    }

    #[test]
    fn entries_and_event_ids_stay_index_aligned() {
        let mut batcher: Batcher<&str> = Batcher::new();
        let first = event("a");
        let second = event("b");
        batcher.push("0", first.id(), 1);
        batcher.push("1", second.id(), 1);
        let (entries, ids) = batcher.take();
        assert_eq!(entries, vec!["0", "1"]);
        assert_eq!(ids, vec![first.id(), second.id()]);
    }

    #[test]
    fn failure_report_resolves_entry_ids_back_to_events() {
        let first = event("a");
        let report = failure_report(
            "send_message_batch",
            &[FailedEntry {
                id: "0",
                code: "InternalError",
                sender_fault: false,
                message: Some("boom"),
            }],
            &[first.id()],
        );
        let text = report.to_string();
        assert!(text.contains(&first.id().to_string()), "{text}");
        assert!(text.contains("InternalError"), "{text}");
    }

    #[test]
    fn oversized_payload_is_rejected() {
        let big = "x".repeat(MAX_PAYLOAD_BYTES + 1);
        let event = Event::create(
            "acme",
            "/orders",
            "order.placed",
            "order-1",
            Payload::from_string(big),
        )
        .expect("valid event");
        assert!(matches!(
            serialize_body(&event),
            Err(Error::InvalidPayload(_))
        ));
    }

    #[test]
    fn serialize_body_round_trips_through_the_wire_format() {
        let event = event("order-1");
        let body = serialize_body(&event).unwrap();
        let decoded = SerializedEvent::from_json_str(&body)
            .unwrap()
            .to_event()
            .unwrap();
        assert_eq!(decoded.id(), event.id());
        assert_eq!(decoded.key().as_str(), "order-1");
    }
}
