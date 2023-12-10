use std::{collections::HashMap, time::SystemTime};

use bytes::{Bytes, BytesMut};
use indexmap::IndexMap;
use kafka_protocol::{messages::{ProduceRequest, TopicName, produce_request::{TopicProduceData, PartitionProduceData}, ProduceResponse}, protocol::Builder, records::{RecordBatchEncoder, Compression, RecordEncodeOptions, Record, TimestampType}};

use crate::{utils::to_kafka_str, io::records::PutRecord, errors::KafkaCallerError};

use super::{CreateRequest, ProcessResponse};

impl CreateRequest<ProduceRequest> for ProduceRequest {
    fn create_request(&self, state: &crate::io::call_state::CallState) -> Result<ProduceRequest, Box<dyn std::error::Error>> {
        let now: i64 = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_millis() as i64;
        
        Ok(
            ProduceRequest::builder()
                .acks(1)
                .transactional_id(None)
                .timeout_ms(30000)
                .topic_data({
                    let mut records_by_topic: HashMap<String, Vec<PutRecord>> = HashMap::default();

                    state.records_to_send
                        .iter()
                        .for_each(|put_record| {
                            records_by_topic
                                .entry(put_record.topic.clone())
                                .and_modify(|vec_put_record| 
                                    vec_put_record.push(put_record.clone())).or_insert(vec!(put_record.clone())
                                );
                        });

                        let mut producer_topic_data = IndexMap::new();

                        for topic_name in records_by_topic.keys() {
                            let partition = 
                                state.broker_metadata
                                    .topics
                                        .get(topic_name)
                                        .ok_or(KafkaCallerError::new(&format!("Could not find topic with name '{}' in stored metadata", topic_name)))?
                                    .partitions
                                        .values()
                                        .by_ref()
                                        .next()
                                        .ok_or(KafkaCallerError::new(&format!("No partitions found for topic '{}' in stored metadata", topic_name)))?;
                
                            let mut partition_produce_data = Vec::<PartitionProduceData>::new();
                
                            let mut record_data = Vec::<Record>::new();
                
                            for (sequence, put_record) in 
                                records_by_topic
                                    .get(topic_name)
                                    .ok_or(KafkaCallerError::new(&format!("No records to put found for topic: '{}'", topic_name)))?
                                    .iter()
                                    .enumerate() 
                            {

                                let mut one_record_data: Record = put_record.into();
                                one_record_data.producer_id = state.producer_id;
                                one_record_data.offset = sequence as i64;
                                one_record_data.sequence = sequence as i32;
                                one_record_data.control = false;
                                one_record_data.timestamp = now;
                                one_record_data.timestamp_type = TimestampType::Creation;
                
                                record_data.push(one_record_data);
                            }
                
                            let encode_options = 
                                RecordEncodeOptions {
                                    compression: Compression::Snappy,
                                    version: 2,
                                };
                
                            let record_bytes = &mut BytesMut::default();
                            RecordBatchEncoder::encode(record_bytes, record_data.iter(), &encode_options)?;
                
                            partition_produce_data.push(
                                PartitionProduceData::builder()
                                    .index(partition.index)
                                    .records(Some(Bytes::copy_from_slice(record_bytes)))
                                    .build()?
                            );
                
                            producer_topic_data.insert(
                                TopicName(to_kafka_str(topic_name)),
                                    TopicProduceData::builder()
                                        .partition_data(partition_produce_data)
                                        .build()?
                            );
                        }

                        producer_topic_data
                })
                .build()?
        )
    }
}

impl ProcessResponse<ProduceResponse> for ProduceResponse {
    fn process_response(&self, _state: &mut crate::io::call_state::CallState) -> Result<(), Box<dyn std::error::Error>> {
        // Produce response is not processed atm ...
        Ok(())
    }
}