use std::num::NonZeroUsize;

use eventuary_core::{Error, Event, EventId, Result, SerializedEvent};

use crate::batch::BatchLimits;

pub(crate) const BATCH_LIMITS: BatchLimits = BatchLimits::new(
    NonZeroUsize::new(10).unwrap(),
    NonZeroUsize::new(256 * 1024).unwrap(),
);

pub(crate) fn serialize_body(event: &Event) -> Result<String> {
    let body = SerializedEvent::from_event(event)?.to_json_string()?;
    if body.len() > BATCH_LIMITS.max_weight() {
        return Err(Error::InvalidPayload(format!(
            "event body {} bytes exceeds the {} byte AWS limit",
            body.len(),
            BATCH_LIMITS.max_weight()
        )));
    }
    Ok(body)
}

#[derive(Debug)]
pub(crate) struct Entry<R> {
    request: R,
    event_id: EventId,
}

impl<R> Entry<R> {
    pub(crate) fn new(request: R, event_id: EventId) -> Self {
        Self { request, event_id }
    }
}

pub(crate) fn unzip<R>(entries: Vec<Entry<R>>) -> (Vec<R>, Vec<EventId>) {
    let mut requests = Vec::with_capacity(entries.len());
    let mut event_ids = Vec::with_capacity(entries.len());
    for entry in entries {
        requests.push(entry.request);
        event_ids.push(entry.event_id);
    }
    (requests, event_ids)
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
    use crate::batch::Batcher;
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

    #[test]
    fn aws_batches_hold_ten_entries_and_256_kb() {
        assert_eq!(BATCH_LIMITS.max_items(), 10);
        assert_eq!(BATCH_LIMITS.max_weight(), 256 * 1024);
    }

    #[test]
    fn a_request_never_separates_from_its_event() {
        let first = event("a");
        let second = event("b");
        let mut batcher: Batcher<Entry<&str>> = Batcher::new(BATCH_LIMITS);
        batcher
            .push(1, |_| Ok(Entry::new("first", first.id())))
            .unwrap();
        batcher
            .push(1, |_| Ok(Entry::new("second", second.id())))
            .unwrap();

        let (requests, event_ids) = unzip(batcher.finish().unwrap().into_items());

        assert_eq!(requests, vec!["first", "second"]);
        assert_eq!(event_ids, vec![first.id(), second.id()]);
    }

    #[test]
    fn half_size_bodies_pair_up_before_the_entry_limit() {
        let mut batcher: Batcher<()> = Batcher::new(BATCH_LIMITS);
        let mut sealed = 0;
        for _ in 0..4 {
            if batcher
                .push(BATCH_LIMITS.max_weight() / 2, |_| Ok(()))
                .unwrap()
                .is_some()
            {
                sealed += 1;
            }
        }

        assert_eq!(sealed, 1);
        assert_eq!(batcher.finish().unwrap().into_items().len(), 2);
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
        let big = "x".repeat(BATCH_LIMITS.max_weight() + 1);
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
