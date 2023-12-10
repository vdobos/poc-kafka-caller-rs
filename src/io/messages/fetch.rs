use std::error::Error;
use kafka_protocol::{messages::{FetchRequest, BrokerId, fetch_request::{FetchTopic, FetchPartition}, TopicName, FetchResponse}, protocol::{Builder, Encodable, Decodable, Message, HeaderVersion}, records::{Record, RecordBatchDecoder}};

use crate::{utils::to_kafka_str, io::call_state::CallState};

use super::CreateRequest;

pub trait ProcessFetchResponse<Res>
    where
        Res: Encodable + Decodable + Default + Message + HeaderVersion,
{
    fn process_response(&self, state: &mut CallState) -> Result<Vec<Record>, Box<dyn Error>>;
}

impl CreateRequest<FetchRequest> for FetchRequest {
    fn create_request(&self, state: &crate::io::call_state::CallState) -> Result<FetchRequest, Box<dyn std::error::Error>> {
        Ok(
            FetchRequest::builder()
                .min_bytes(8)
                .max_bytes(4194304)
                .max_wait_ms(1000)
                .cluster_id(Some(to_kafka_str(&state.broker_metadata.cluster_id)))
                .replica_id(BrokerId(-1))
                .topics(
                    state.fetch_state
                        .iter()
                        .map(|(name, commited_offsets)|
                            FetchTopic::builder()
                                .topic(TopicName(to_kafka_str(name)))
                                .topic_id(
                                    state.broker_metadata.topics
                                        .get(name)
                                        .unwrap()
                                            .id
                                )
                                .partitions(
                                    commited_offsets
                                        .iter()
                                        .filter(|(_, offset_state)| -> bool {
                                            offset_state.error_code == 0
                                        })
                                        .map(|(index, offset_state)|
                                            FetchPartition::builder()
                                                .partition(*index)
                                                .fetch_offset(
                                                    if offset_state.commited_offset == -1 {
                                                        0
                                                    } else {
                                                        offset_state.commited_offset
                                                    } 
                                                )
                                                .current_leader_epoch(0)
                                                .last_fetched_epoch(-1)
                                                .partition_max_bytes(1048576)
                                                .build()
                                                .unwrap()
                                        )
                                        .collect()
                                    )
                                .build()
                                .unwrap()
                        )
                        .collect()
                )
                .build()?
        )
    }
}

impl ProcessFetchResponse<FetchResponse> for FetchResponse {
    fn process_response(&self, state: &mut CallState) -> Result<Vec<Record>, Box<dyn Error>> {
        let mut out_records = Vec::<Record>::new();

        for fetchable_topic_response in &self.responses {
            let topic_name = state.broker_metadata.topic_name_from_id(fetchable_topic_response.topic_id)?;

            fetchable_topic_response.partitions
                .iter()
                .for_each(|partition_data| {
                    let partition_offset_state = 
                        state.fetch_state
                            .get_mut(&topic_name)
                            .unwrap()
                            .get_mut(&partition_data.partition_index)
                            .unwrap();
                    
                    partition_offset_state.error_code = partition_data.error_code;

                    let mut partition_records = 
                        RecordBatchDecoder::decode(
                            partition_data.records
                                .clone()
                                .as_mut()
                                .unwrap()
                        )
                        .unwrap();

                        partition_offset_state.polled_offset = 
                            partition_records
                                .iter()
                                .map(|record| record.offset)
                                .max()
                                .unwrap_or(-1);

                        out_records.append(&mut partition_records);
                });
        }

        Ok(out_records)
    }
}