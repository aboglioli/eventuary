use std::mem;

use crate::event::Event;
use crate::payload::Payload;

#[derive(Debug, Clone)]
pub struct EventCollector<P = Payload> {
    events: Vec<Event<P>>,
}

impl<P> EventCollector<P> {
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }

    pub fn with(event: Event<P>) -> Self {
        Self {
            events: vec![event],
        }
    }

    pub fn collect(&mut self, event: Event<P>) {
        self.events.push(event);
    }

    pub fn drain(&mut self) -> Vec<Event<P>> {
        mem::take(&mut self.events)
    }

    pub fn list(&self) -> &[Event<P>] {
        &self.events
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Event<P>> {
        self.events.iter()
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

impl<P> Default for EventCollector<P> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev() -> Event {
        Event::create(
            "acme",
            "/x",
            "thing.happened",
            crate::Payload::from_string("p"),
        )
        .unwrap()
    }

    #[test]
    fn new_is_empty() {
        let c: EventCollector = EventCollector::new();
        assert!(c.is_empty());
        assert_eq!(c.len(), 0);
        assert!(c.list().is_empty());
    }

    #[test]
    fn with_single_event() {
        let c = EventCollector::with(ev());
        assert_eq!(c.len(), 1);
        assert!(!c.is_empty());
    }

    #[test]
    fn collect_adds_events() {
        let mut c = EventCollector::new();
        c.collect(ev());
        c.collect(ev());
        assert_eq!(c.len(), 2);
        assert!(!c.is_empty());
    }

    #[test]
    fn drain_removes_and_returns() {
        let mut c = EventCollector::new();
        c.collect(ev());

        let drained = c.drain();
        assert_eq!(drained.len(), 1);
        assert!(c.is_empty());
    }

    #[test]
    fn drain_twice_returns_empty_second() {
        let mut c = EventCollector::new();
        c.collect(ev());

        let first = c.drain();
        assert_eq!(first.len(), 1);

        let second = c.drain();
        assert!(second.is_empty());
    }

    #[test]
    fn iter_yields_collected() {
        let mut c = EventCollector::new();
        c.collect(ev());
        c.collect(ev());

        assert_eq!(c.iter().count(), 2);
    }

    #[test]
    fn default_is_empty() {
        let c: EventCollector = EventCollector::default();
        assert!(c.is_empty());
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct UserUpdated {
        user_id: String,
    }

    #[test]
    fn typed_collector_new_is_empty() {
        let collector: EventCollector<UserUpdated> = EventCollector::new();
        assert!(collector.is_empty());
    }

    #[test]
    fn collector_drains_typed_events() {
        let event: Event<UserUpdated> = Event::create(
            "org",
            "/users",
            "user.updated",
            UserUpdated {
                user_id: "u-1".to_owned(),
            },
        )
        .unwrap();

        let mut collector = EventCollector::with(event);
        let drained: Vec<Event<UserUpdated>> = collector.drain();

        assert_eq!(drained[0].payload().user_id, "u-1");
    }
}
