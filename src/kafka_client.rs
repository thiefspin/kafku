use kafka::client::metadata::Topic;
use kafka::client::{KafkaClient, PartitionOffset};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::producer::{Producer, Record, RequiredAcks};
use std::fmt::Write;
use std::str;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Partition {
    pub id: i32,
    pub leader: String,
    pub available: bool,
    pub offset: i64
}
#[derive(Debug, Clone)]
pub struct TopicData {
    pub name: String,
    pub partitions: Vec<Partition>,
}

pub struct SimpleKafkaClient {
    pub hosts: Vec<String>,
}

impl SimpleKafkaClient {
    pub fn create(&self) -> KafkaClient {
        KafkaClient::new(self.hosts.clone())
    }

    pub fn list_topics(&self) -> Vec<String> {
        let mut client = self.create();
        client.load_metadata_all().unwrap();
        return client
            .topics()
            .iter()
            .map(|topic: Topic| topic.name().to_string())
            .collect();
    }

    pub fn list_brokers(&self) -> Vec<String> {
        let mut client = self.create();
        return client.hosts().to_owned();
    }

    fn get_offsets(&self, topic: String ) -> Vec<PartitionOffset> {
        let mut client = self.create();
        client.load_metadata_all().unwrap();
        return client.fetch_topic_offsets(topic, FetchOffset::Latest).unwrap();
    }

    pub fn list_topic_details(&self) -> Vec<TopicData> {
        let mut client = self.create();
        client.load_metadata_all().unwrap();
        client
            .topics()
            .iter()
            .map(|topic| {
                let offsets = self.get_offsets(topic.name().to_string());
                let partitions = topic
                    .partitions()
                    .iter()
                    .map(|p| {
                        let offset = offsets.iter().find(|o| o.partition == p.id());
                        Partition {
                        id: p.id(),
                        leader: p
                            .leader()
                            .map(|l| l.host())
                            .unwrap_or("No Leader Available")
                            .to_string(),
                        available: p.is_available(),
                        offset: offset.map(|o| o.offset).unwrap_or(0)
                        }
                    })
                    .collect();
                TopicData {
                    name: topic.name().to_string(),
                    partitions: partitions,
                }
            })
            .collect()
    }

    pub fn create_consumer(&self, topic: &str) -> Consumer {
        // println!("Consumer group set to {}", whoami::username());
        Consumer::from_hosts(self.hosts.clone())
        .with_topic(topic.to_owned())
            // .with_topic_partitions(topic.to_owned(), &[partition])
            .with_fallback_offset(FetchOffset::Earliest)
            // .with_group(whoami::username().to_owned())
            .with_group("somegroup3".to_owned())
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()
            .unwrap()
    }

    pub fn start_consumer(&self, mut consumer: Consumer, f: &mut dyn FnMut(std::string::String)) {
        loop {
            for ms in consumer.poll().unwrap().iter() {
                for m in ms.messages() {
                    let message = parse_message(m.value);
                    f(message)
                }
                consumer.consume_messageset(ms).unwrap();
            }
            consumer.commit_consumed().unwrap();
        }
    }

    pub fn create_producer(&self) -> Producer {
        return Producer::from_hosts(self.hosts.clone())
            .with_ack_timeout(Duration::from_secs(10))
            .with_required_acks(RequiredAcks::One)
            .create()
            .unwrap();
    }

    pub fn produce(&self, mut producer: Producer, topic: String, msg: String) {
        let mut buf = String::with_capacity(2);
        let _ = write!(&mut buf, "{}", msg);
        producer
            .send(&Record::from_value(&topic, buf.as_bytes()))
            .unwrap();
        buf.clear();
    }
}

pub fn parse_message(message_bytes: &[u8]) -> String {
    str::from_utf8(&message_bytes).unwrap().to_owned()
}
