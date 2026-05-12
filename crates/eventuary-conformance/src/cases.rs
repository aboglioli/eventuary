use std::collections::HashSet;
use std::time::Duration;

use chrono::Utc;
use eventuary_core::io::Writer;
use eventuary_core::{
    ConsumerGroupId, Event, Namespace, OrganizationId, Payload, StartFrom, Topic,
};

use crate::factory::{Backend, ReaderRequest};

const READ_TIMEOUT: Duration = Duration::from_secs(5);

fn unique_organization() -> OrganizationId {
    OrganizationId::new(format!("conf-{}", uuid::Uuid::now_v7())).expect("valid organization id")
}

fn make_event(org: &OrganizationId, namespace: &str, topic: &str, key: &str) -> Event {
    Event::builder(
        org.as_str(),
        namespace,
        topic,
        Payload::from_string(format!("payload-{key}")),
    )
    .expect("valid event")
    .key(key)
    .expect("valid key")
    .build()
    .expect("valid event")
}

pub async fn case_write_read_roundtrip(backend: &dyn Backend) {
    let org = unique_organization();
    let writer = backend.writer().await;
    let event = make_event(&org, "/x", "thing.happened", "k0");
    writer.write(&event).await.expect("write");

    let mut request = ReaderRequest::for_organization(org.clone());
    request.start_from = StartFrom::Earliest;
    let received = backend
        .read_one(request, READ_TIMEOUT)
        .await
        .expect("read one event");
    assert_eq!(
        received.event.id(),
        event.id(),
        "roundtrip: id should match"
    );
    assert_eq!(
        received.event.topic().as_str(),
        event.topic().as_str(),
        "roundtrip: topic should match"
    );
    assert_eq!(
        received.event.key().expect("event has key").as_str(),
        event.key().expect("event has key").as_str(),
        "roundtrip: key should match"
    );
    assert_eq!(
        received.event.payload().data(),
        event.payload().data(),
        "roundtrip: payload should match"
    );
    (received.ack)().await.expect("ack");
}

pub async fn case_all_organizations_read(backend: &dyn Backend) {
    let org_a = unique_organization();
    let org_b = unique_organization();
    let namespace =
        Namespace::new(format!("/all-org/{}", uuid::Uuid::now_v7())).expect("valid namespace");
    let writer = backend.writer().await;
    let event_a = make_event(&org_a, namespace.as_str(), "thing.happened", "org-a");
    let event_b = make_event(&org_b, namespace.as_str(), "thing.happened", "org-b");
    writer.write(&event_a).await.expect("write org a");
    writer.write(&event_b).await.expect("write org b");

    let mut request = ReaderRequest::new();
    request.namespace = Some(namespace);
    request.start_from = StartFrom::Earliest;
    let received = backend.read_many(request, 2, READ_TIMEOUT).await;
    assert_eq!(
        received.len(),
        2,
        "all_organizations_read: expected events from both organizations"
    );
    let organizations: HashSet<String> = received
        .iter()
        .map(|c| c.event.organization().as_str().to_owned())
        .collect();
    assert_eq!(
        organizations,
        HashSet::from([org_a.as_str().to_owned(), org_b.as_str().to_owned()])
    );
}

pub async fn case_write_all_preserves_all_events(backend: &dyn Backend) {
    let org = unique_organization();
    let writer = backend.writer().await;
    let events: Vec<Event> = (0..5)
        .map(|i| make_event(&org, "/x", "thing.happened", &format!("k{i}")))
        .collect();
    writer.write_all(&events).await.expect("write_all");

    let mut request = ReaderRequest::for_organization(org.clone());
    request.start_from = StartFrom::Earliest;
    let received = backend.read_many(request, events.len(), READ_TIMEOUT).await;
    assert_eq!(
        received.len(),
        events.len(),
        "write_all_preserves_all_events: expected {} events, got {}",
        events.len(),
        received.len()
    );
    let expected: HashSet<String> = events
        .iter()
        .map(|e| e.key().expect("event has key").as_str().to_owned())
        .collect();
    let actual: HashSet<String> = received
        .iter()
        .map(|c| c.event.key().expect("event has key").as_str().to_owned())
        .collect();
    assert_eq!(actual, expected, "all events must be delivered");
}

pub async fn case_ordering_preserved(backend: &dyn Backend) {
    let org = unique_organization();
    let writer = backend.writer().await;
    let events: Vec<Event> = (0..10)
        .map(|i| make_event(&org, "/x", "thing.happened", &format!("k{i:02}")))
        .collect();
    for event in &events {
        writer.write(event).await.expect("write");
    }

    let mut request = ReaderRequest::for_organization(org.clone());
    request.start_from = StartFrom::Earliest;
    let received = backend.read_many(request, events.len(), READ_TIMEOUT).await;
    assert_eq!(received.len(), events.len(), "expected all events");
    let expected_keys: Vec<String> = events
        .iter()
        .map(|e| e.key().expect("event has key").as_str().to_owned())
        .collect();
    let actual_keys: Vec<String> = received
        .iter()
        .map(|c| c.event.key().expect("event has key").as_str().to_owned())
        .collect();
    assert_eq!(
        actual_keys, expected_keys,
        "ordering_preserved: keys must be in write order"
    );
}

pub async fn case_start_from_earliest(backend: &dyn Backend) {
    let org = unique_organization();
    let writer = backend.writer().await;
    for i in 0..3 {
        writer
            .write(&make_event(&org, "/x", "thing.happened", &format!("k{i}")))
            .await
            .expect("write");
    }

    let mut request = ReaderRequest::for_organization(org.clone());
    request.start_from = StartFrom::Earliest;
    let received = backend.read_many(request, 3, READ_TIMEOUT).await;
    assert_eq!(
        received.len(),
        3,
        "start_from_earliest: expected 3 replayed events, got {}",
        received.len()
    );
}

pub async fn case_start_from_latest(backend: &dyn Backend) {
    let org = unique_organization();
    let writer = backend.writer().await;
    for i in 0..3 {
        writer
            .write(&make_event(
                &org,
                "/x",
                "thing.happened",
                &format!("old{i}"),
            ))
            .await
            .expect("write");
    }

    let mut request = ReaderRequest::for_organization(org.clone());
    request.start_from = StartFrom::Latest;

    let backend_ref = backend;
    let writer_ref = &writer;
    let writer_task = async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        writer_ref
            .write(&make_event(&org, "/x", "thing.happened", "new"))
            .await
            .expect("write new");
    };
    let read_task = async move { backend_ref.read_one(request, READ_TIMEOUT).await };
    let (_, received) = tokio::join!(writer_task, read_task);

    let received = received.expect("expected new event after latest");
    assert_eq!(
        received.event.key().expect("event has key").as_str(),
        "new",
        "start_from_latest: should receive only the new event"
    );
}

pub async fn case_start_from_timestamp(backend: &dyn Backend) {
    let org = unique_organization();
    let writer = backend.writer().await;
    writer
        .write(&make_event(&org, "/x", "thing.happened", "before"))
        .await
        .expect("write before");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let cutoff = Utc::now();
    tokio::time::sleep(Duration::from_millis(50)).await;
    writer
        .write(&make_event(&org, "/x", "thing.happened", "after"))
        .await
        .expect("write after");

    let mut request = ReaderRequest::for_organization(org.clone());
    request.start_from = StartFrom::Timestamp(cutoff);
    let received = backend
        .read_one(request, READ_TIMEOUT)
        .await
        .expect("expected event after timestamp");
    assert_eq!(
        received.event.key().expect("event has key").as_str(),
        "after",
        "start_from_timestamp: should receive only post-cutoff event"
    );
}

pub async fn case_topic_filter(backend: &dyn Backend) {
    let org = unique_organization();
    let writer = backend.writer().await;
    writer
        .write(&make_event(&org, "/x", "task.created", "t1"))
        .await
        .expect("write");
    writer
        .write(&make_event(&org, "/x", "task.completed", "t2"))
        .await
        .expect("write");
    writer
        .write(&make_event(&org, "/x", "task.created", "t3"))
        .await
        .expect("write");

    let mut request = ReaderRequest::for_organization(org.clone());
    request.start_from = StartFrom::Earliest;
    request.topics = vec![Topic::new("task.created").expect("valid topic")];
    let received = backend.read_many(request, 2, READ_TIMEOUT).await;
    assert_eq!(received.len(), 2, "topic_filter: expected 2 events");
    for c in &received {
        assert_eq!(
            c.event.topic().as_str(),
            "task.created",
            "topic_filter: only matching events should be delivered"
        );
    }
    let keys: HashSet<String> = received
        .iter()
        .map(|c| c.event.key().expect("event has key").as_str().to_owned())
        .collect();
    assert_eq!(keys, HashSet::from(["t1".to_owned(), "t3".to_owned()]));
}

pub async fn case_namespace_prefix_filter(backend: &dyn Backend) {
    let org = unique_organization();
    let writer = backend.writer().await;
    writer
        .write(&make_event(&org, "/backend", "thing.happened", "b1"))
        .await
        .expect("write");
    writer
        .write(&make_event(&org, "/frontend", "thing.happened", "f1"))
        .await
        .expect("write");
    writer
        .write(&make_event(&org, "/backend/auth", "thing.happened", "b2"))
        .await
        .expect("write");

    let mut request = ReaderRequest::for_organization(org.clone());
    request.start_from = StartFrom::Earliest;
    request.namespace = Some(Namespace::new("/backend").expect("valid namespace"));
    let received = backend.read_many(request, 2, READ_TIMEOUT).await;
    assert_eq!(
        received.len(),
        2,
        "namespace_prefix_filter: expected 2 events"
    );
    let keys: HashSet<String> = received
        .iter()
        .map(|c| c.event.key().expect("event has key").as_str().to_owned())
        .collect();
    assert_eq!(keys, HashSet::from(["b1".to_owned(), "b2".to_owned()]));
}

pub async fn case_ack_advances_checkpoint(backend: &dyn Backend) {
    let org = unique_organization();
    let writer = backend.writer().await;
    for i in 0..3 {
        writer
            .write(&make_event(&org, "/x", "thing.happened", &format!("k{i}")))
            .await
            .expect("write");
    }

    let group = ConsumerGroupId::new("conf-group").expect("valid group id");

    let mut request = ReaderRequest::for_organization(org.clone());
    request.start_from = StartFrom::Earliest;
    request.consumer_group_id = Some(group.clone());

    let first = backend
        .read_one(request, READ_TIMEOUT)
        .await
        .expect("expected first event");
    assert_eq!(first.event.key().expect("event has key").as_str(), "k0");
    (first.ack)().await.expect("ack");

    let mut resume_request = ReaderRequest::for_organization(org);
    resume_request.start_from = StartFrom::Earliest;
    resume_request.consumer_group_id = Some(group);
    let remaining = backend.read_many(resume_request, 2, READ_TIMEOUT).await;
    assert_eq!(
        remaining.len(),
        2,
        "ack_advances_checkpoint: expected 2 remaining events after restart"
    );
    assert!(
        !remaining
            .iter()
            .any(|c| c.event.key().expect("event has key").as_str() == "k0"),
        "ack_advances_checkpoint: k0 must not be redelivered"
    );
}

pub async fn case_nack_does_not_advance_checkpoint(backend: &dyn Backend) {
    let org = unique_organization();
    let writer = backend.writer().await;
    writer
        .write(&make_event(&org, "/x", "thing.happened", "k0"))
        .await
        .expect("write");

    let group = ConsumerGroupId::new("conf-group").expect("valid group id");

    let mut request = ReaderRequest::for_organization(org.clone());
    request.start_from = StartFrom::Earliest;
    request.consumer_group_id = Some(group.clone());

    let first = backend
        .read_one(request, READ_TIMEOUT)
        .await
        .expect("expected first event");
    assert_eq!(first.event.key().expect("event has key").as_str(), "k0");
    (first.nack)().await.expect("nack");

    let mut resume_request = ReaderRequest::for_organization(org);
    resume_request.start_from = StartFrom::Earliest;
    resume_request.consumer_group_id = Some(group);
    let again = backend
        .read_one(resume_request, READ_TIMEOUT)
        .await
        .expect("expected redelivery after nack");
    assert_eq!(
        again.event.key().expect("event has key").as_str(),
        "k0",
        "nack_does_not_advance_checkpoint: must redeliver k0"
    );
}

pub async fn case_independent_consumer_groups(backend: &dyn Backend) {
    let org = unique_organization();
    let writer = backend.writer().await;
    for i in 0..3 {
        writer
            .write(&make_event(&org, "/x", "thing.happened", &format!("k{i}")))
            .await
            .expect("write");
    }

    let group_a = ConsumerGroupId::new("group-a").expect("valid group id");
    let group_b = ConsumerGroupId::new("group-b").expect("valid group id");

    let mut request_a = ReaderRequest::for_organization(org.clone());
    request_a.start_from = StartFrom::Earliest;
    request_a.consumer_group_id = Some(group_a);
    let a_events = backend.read_many(request_a, 3, READ_TIMEOUT).await;
    assert_eq!(a_events.len(), 3);
    for e in a_events {
        (e.ack)().await.expect("ack");
    }

    let mut request_b = ReaderRequest::for_organization(org);
    request_b.start_from = StartFrom::Earliest;
    request_b.consumer_group_id = Some(group_b);
    let b_events = backend.read_many(request_b, 3, READ_TIMEOUT).await;
    assert_eq!(
        b_events.len(),
        3,
        "independent_consumer_groups: group B must see all events"
    );
}

pub async fn case_independent_checkpoints_within_group(backend: &dyn Backend) {
    let org = unique_organization();
    let writer = backend.writer().await;
    for i in 0..2 {
        writer
            .write(&make_event(&org, "/x", "thing.happened", &format!("k{i}")))
            .await
            .expect("write");
    }

    let group = ConsumerGroupId::new("shared-group").expect("valid group id");

    let mut request_s1 = ReaderRequest::for_organization(org.clone());
    request_s1.start_from = StartFrom::Earliest;
    request_s1.consumer_group_id = Some(group.clone());
    request_s1.checkpoint_name = "checkpoint-one".to_owned();
    let s1 = backend.read_many(request_s1, 2, READ_TIMEOUT).await;
    assert_eq!(s1.len(), 2);
    for e in s1 {
        (e.ack)().await.expect("ack");
    }

    let mut request_s2 = ReaderRequest::for_organization(org);
    request_s2.start_from = StartFrom::Earliest;
    request_s2.consumer_group_id = Some(group);
    request_s2.checkpoint_name = "checkpoint-two".to_owned();
    let s2 = backend.read_many(request_s2, 2, READ_TIMEOUT).await;
    assert_eq!(
        s2.len(),
        2,
        "independent_checkpoints_within_group: checkpoint-two offset must be independent"
    );
}

pub async fn run_all(backend: &dyn Backend) {
    let caps = backend.capabilities();
    case_write_read_roundtrip(backend).await;
    case_all_organizations_read(backend).await;
    case_write_all_preserves_all_events(backend).await;
    if caps.preserves_total_order {
        case_ordering_preserved(backend).await;
    }
    if caps.supports_replay {
        case_start_from_earliest(backend).await;
    }
    case_start_from_latest(backend).await;
    if caps.supports_timestamp_start {
        case_start_from_timestamp(backend).await;
    }
    case_topic_filter(backend).await;
    case_namespace_prefix_filter(backend).await;
    if caps.supports_consumer_groups {
        case_ack_advances_checkpoint(backend).await;
        case_nack_does_not_advance_checkpoint(backend).await;
        case_independent_consumer_groups(backend).await;
    }
    if caps.supports_independent_checkpoints {
        case_independent_checkpoints_within_group(backend).await;
    }
}
