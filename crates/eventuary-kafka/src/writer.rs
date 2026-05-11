use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

use eventuary_core::io::Writer;
use eventuary_core::{Error, Event, Result, SerializedEvent};

const SEND_TIMEOUT: Duration = Duration::from_secs(5);

pub struct KafkaWriter {
    producer: FutureProducer,
    topic: String,
}

impl KafkaWriter {
    pub fn new(brokers: &[String], topic: impl Into<String>) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers.join(","))
            .set("message.timeout.ms", "5000")
            .create()
            .map_err(|e| Error::Store(e.to_string()))?;
        Ok(Self {
            producer,
            topic: topic.into(),
        })
    }

    fn body(event: &Event) -> Result<String> {
        let s = SerializedEvent::from_event(event)?;
        s.to_json_string()
    }
}

impl Writer for KafkaWriter {
    async fn write(&self, event: &Event) -> Result<()> {
        let body = Self::body(event)?;
        if let Some(key) = event.key() {
            let key = key.as_str().to_owned();
            let record: FutureRecord<String, String> =
                FutureRecord::to(&self.topic).payload(&body).key(&key);
            self.producer
                .send(record, SEND_TIMEOUT)
                .await
                .map_err(|(e, _)| Error::Store(e.to_string()))?;
            return Ok(());
        }
        let record: FutureRecord<String, String> = FutureRecord::to(&self.topic).payload(&body);
        self.producer
            .send(record, SEND_TIMEOUT)
            .await
            .map_err(|(e, _)| Error::Store(e.to_string()))?;
        Ok(())
    }

    async fn write_all(&self, events: &[Event]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let payloads: Vec<(String, Option<String>)> = events
            .iter()
            .map(|e| Ok::<_, Error>((Self::body(e)?, e.key().map(|key| key.to_string()))))
            .collect::<Result<_>>()?;

        for (body, key) in &payloads {
            match key {
                Some(key) => {
                    let record: FutureRecord<String, String> =
                        FutureRecord::to(&self.topic).payload(body).key(key);
                    self.producer
                        .send(record, SEND_TIMEOUT)
                        .await
                        .map_err(|(e, _)| Error::Store(e.to_string()))?;
                }
                None => {
                    let record: FutureRecord<String, String> =
                        FutureRecord::to(&self.topic).payload(body);
                    self.producer
                        .send(record, SEND_TIMEOUT)
                        .await
                        .map_err(|(e, _)| Error::Store(e.to_string()))?;
                }
            }
        }
        Ok(())
    }
}
