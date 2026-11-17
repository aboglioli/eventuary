use crate::error::Result;
use crate::io::filter::{EventFilter, TopicPattern};
use crate::io::{ArcHandler, ConsumerGroupId, Handler, HandlerExt};
use crate::{OrganizationId, Topic};

/// A handler bound to what it consumes: its consumer group (the handler's id),
/// the filter of one organization and a set of topics, and whether a group with
/// no checkpoint starts at the end of the log.
///
/// It is a plain description. A runtime reads with [`Subscriber::filter`] and
/// resumes by [`Subscriber::group`]; it does not know any backend.
#[derive(Clone)]
pub struct Subscriber {
    group: ConsumerGroupId,
    handler: ArcHandler,
    filter: EventFilter,
    is_from_latest: bool,
}

impl Subscriber {
    pub fn new(
        handler: impl Handler + 'static,
        organization: OrganizationId,
        topics: &[&str],
    ) -> Result<Self> {
        let group = ConsumerGroupId::new(handler.id())?;
        let topics = topics
            .iter()
            .map(|topic| Topic::new(*topic))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            group,
            handler: handler.into_arced(),
            filter: EventFilter {
                topic: Some(TopicPattern::any_of(topics)?),
                ..EventFilter::for_organization(organization)
            },
            is_from_latest: false,
        })
    }

    pub fn platform(handler: impl Handler + 'static, topics: &[&str]) -> Result<Self> {
        Self::new(handler, OrganizationId::platform(), topics)
    }

    pub fn from_latest(mut self) -> Self {
        self.is_from_latest = true;
        self
    }

    pub fn is_from_latest(&self) -> bool {
        self.is_from_latest
    }

    pub fn group(&self) -> &ConsumerGroupId {
        &self.group
    }

    pub fn handler(&self) -> &ArcHandler {
        &self.handler
    }

    pub fn filter(&self) -> &EventFilter {
        &self.filter
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::Filter;
    use crate::{Event, Payload};

    struct Named(&'static str);

    impl Handler for Named {
        fn id(&self) -> &str {
            self.0
        }

        async fn handle(&self, _: &Event) -> Result<()> {
            Ok(())
        }
    }

    fn event(organization: &str, topic: &str) -> Event {
        Event::create(
            organization,
            "/sample",
            topic,
            "k-1",
            Payload::from_string("{}"),
        )
        .unwrap()
    }

    #[test]
    fn a_subscriber_reads_only_its_topics_of_its_organization() {
        let subscriber = Subscriber::new(
            Named("mailer"),
            OrganizationId::new("acme").unwrap(),
            &["user.registered", "user.email_verified"],
        )
        .unwrap();

        assert_eq!(subscriber.group().as_str(), "mailer");
        assert!(
            subscriber
                .filter()
                .matches(&event("acme", "user.registered"))
        );
        assert!(
            subscriber
                .filter()
                .matches(&event("acme", "user.email_verified"))
        );
        assert!(
            !subscriber
                .filter()
                .matches(&event("acme", "user.password_reset"))
        );
        assert!(
            !subscriber
                .filter()
                .matches(&event("other", "user.registered"))
        );
    }

    #[test]
    fn the_platform_subscriber_reads_the_platform_organization() {
        let subscriber = Subscriber::platform(Named("mailer"), &["user.registered"]).unwrap();

        assert!(
            subscriber
                .filter()
                .matches(&event("_platform", "user.registered"))
        );
        assert!(
            !subscriber
                .filter()
                .matches(&event("acme", "user.registered"))
        );
    }

    #[test]
    fn a_subscriber_reads_at_least_one_valid_topic() {
        assert!(Subscriber::platform(Named("mailer"), &[]).is_err());
        assert!(Subscriber::platform(Named("mailer"), &["Not A Topic"]).is_err());
    }

    #[test]
    fn a_subscriber_is_named_as_a_valid_consumer_group() {
        assert!(Subscriber::platform(Named("Not A Group"), &["user.registered"]).is_err());
        assert!(Subscriber::platform(Named(""), &["user.registered"]).is_err());
    }

    #[test]
    fn a_subscriber_starts_from_its_checkpoint_unless_it_asks_for_the_latest() {
        let subscriber = Subscriber::platform(Named("mailer"), &["user.registered"]).unwrap();

        assert!(!subscriber.is_from_latest());
        assert!(subscriber.from_latest().is_from_latest());
    }

    #[test]
    fn a_cloned_subscriber_keeps_its_group_filter_and_handler() {
        let subscriber = Subscriber::platform(Named("mailer"), &["user.registered"])
            .unwrap()
            .from_latest();

        let copy = subscriber.clone();

        assert_eq!(copy.group(), subscriber.group());
        assert_eq!(copy.handler().id(), "mailer");
        assert!(copy.is_from_latest());
        assert!(
            copy.filter()
                .matches(&event("_platform", "user.registered"))
        );
    }
}
