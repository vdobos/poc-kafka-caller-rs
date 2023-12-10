use std::collections::HashMap;
use std::error::Error;
use std::ops::RangeInclusive;
use std::sync::{Arc, atomic::AtomicI32};
use bytes::Bytes;
use uuid::Uuid;
use crate::Configuration;
use crate::errors::KafkaCallerError;

use super::records::PutRecord;

#[derive(Debug)]
pub(in super::super) struct CallState {
    pub configuration: Configuration,
    pub correlation_id: Arc<AtomicI32>,
    pub broker_api_versions: HashMap<i16, RangeInclusive<i16>>,
    pub connected_topics: Vec<String>,
    pub broker_metadata: BrokerMetadata,
    pub coordinators: HashMap<String, Coordinator>,
    pub group_subscription: GroupSubscription,
    pub fetch_state: HashMap<String, HashMap<i32, PartitionOffsetState>>,
    pub producer_id: i64,
    pub records_to_send: Vec<PutRecord>,
}

impl CallState {
    pub fn new(configuration: &Configuration) -> Result<Self, Box<dyn Error>> {
        Ok (
            Self {
                configuration: configuration.clone(),
                correlation_id: Arc::new(AtomicI32::new(0)),
                broker_api_versions: HashMap::new(),
                connected_topics: Vec::new(),
                broker_metadata: BrokerMetadata::default(),
                coordinators: HashMap::new(),
                group_subscription: GroupSubscription::default(),
                fetch_state: HashMap::new(),
                producer_id: -1,
                records_to_send: Vec::new(),
            }
        )
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct BrokerMetadata {
    pub cluster_id: String,
    pub controller_id: i32,
    pub brokers: HashMap<i32, Broker>,
    pub topics: HashMap<String, Topic>,
}

impl Default for BrokerMetadata {
    fn default() -> Self {
        Self {
            cluster_id: String::default(),
            controller_id: -1,
            brokers: HashMap::new(),
            topics: HashMap::new(),
        }
    }
}

impl BrokerMetadata {
    pub(super) fn topic_name_from_id(&self, topic_id: Uuid) -> Result<String, KafkaCallerError> {
        self.topics
            .values()
            .find_map(|topic| {
                topic.id.eq(&topic_id).then(|| topic.name.clone())
            })
            .ok_or(KafkaCallerError::new(&format!("Could not find topic name for id: '{}'", topic_id)))
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct Broker {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone)]
pub(crate) struct Topic {
    pub id: Uuid,
    pub name: String,
    pub partitions: HashMap<i32, Partition>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct Partition {
    pub index: i32,
    pub leader_id: i32,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct Coordinator {
    pub group: String,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone)]
pub(crate) struct GroupSubscription {
    pub member_id: String,
    pub leader_id: String,
    pub generation_id: i32,
    pub subscriptions: Vec<SubscriptionMember>,
}

impl Default for GroupSubscription {
    fn default() -> Self {
        Self {
            member_id: String::default(),
            leader_id: String::default(),
            generation_id: -1,
            subscriptions: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SubscriptionMember {
    pub member_id: String,
    pub metadata: Bytes,
}

#[derive(Debug, Clone)]
pub(crate) struct PartitionOffsetState {
    pub commited_offset: i64,
    pub polled_offset: i64,
    pub error_code: i16,
}

impl PartitionOffsetState {
    pub(super) fn new(index: i64) -> Self {
        Self {
            commited_offset: index,
            polled_offset: -1,
            error_code: 0,
        }
    }
}