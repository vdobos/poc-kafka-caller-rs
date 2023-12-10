use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use kafka_protocol::records::{Record, TimestampType};

use crate::utils::to_kafka_str;

#[derive(Debug, Clone)]
pub struct PutRecord {
    pub topic: String,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: HashMap<String, Option<Bytes>>,
}

impl PutRecord {
    fn new(topic: &str) -> Self {
        Self {
            topic: String::from(topic),
            key: None,
            value: None,
            headers: HashMap::default(),       
        }
    }

    pub fn new_with_key_str(topic: &str, key: &str) -> Self {
        let mut out_new_instance = Self::new(topic);
        out_new_instance.set_key_str(key);

        out_new_instance
    }

    pub fn new_with_key_value_str(topic: &str, key: &str, value: &str) -> Self {
        let mut out_new_instance = Self::new_with_key_str(topic, key);
        out_new_instance.set_value_str(value);

        out_new_instance
    }

    pub fn set_key_str(&mut self, key: &str) {
        self.key = Some(Bytes::copy_from_slice(key.as_bytes()));
    }

    pub fn set_value_str(&mut self, value: &str) {
        self.value = Some(Bytes::copy_from_slice(value.as_bytes()));
    }

    pub fn set_str(&mut self, key: &str, value: &str) {
        self.set_key_str(key);
        self.set_value_str(value);
    }

    pub fn add_header_with_str_key(&mut self, key: &str, value: &[u8]) {
        self.headers.insert(String::from(key), Some(Bytes::copy_from_slice(value)));
    }
}

impl From<&PutRecord> for Record {
    fn from(put_record: &PutRecord) -> Self {
        Record { 
            transactional: false, 
            control: false, 
            partition_leader_epoch: -1, 
            producer_id: -1, 
            producer_epoch: -1, 
            timestamp_type: TimestampType::Creation, 
            offset: -1,
            sequence: -1, 
            timestamp: -1,
            key: put_record.key.clone(), 
            value: put_record.value.clone(), 
            headers: 
                put_record.headers
                    .iter()
                    .map(|(key, value)|
                        (to_kafka_str(key), value.clone())
                    )   
                    .collect(),
        }
    }
}

pub(in super::super) fn extract_topics(records: &[PutRecord]) -> Vec<String> {
    records
        .iter()
        .map(|put_record|
            put_record.topic.clone()
        )
        .collect::<HashSet<String>>()
        .into_iter()
        .collect()
}