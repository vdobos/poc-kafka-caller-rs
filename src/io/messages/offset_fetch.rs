use std::collections::HashMap;

use kafka_protocol::{messages::{OffsetFetchRequest, offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopics}, TopicName, GroupId, OffsetFetchResponse, offset_fetch_response::OffsetFetchResponseTopics}, protocol::Builder};

use crate::{io::call_state::PartitionOffsetState, utils::to_kafka_str, errors::KafkaCallerError};

use super::{CreateRequest, ProcessResponse};

impl CreateRequest<OffsetFetchRequest> for OffsetFetchRequest {
    fn create_request(&self, state: &crate::io::call_state::CallState) -> Result<OffsetFetchRequest, Box<dyn std::error::Error>> {
        Ok(
            OffsetFetchRequest::builder()
                .groups(
                    vec!(
                        OffsetFetchRequestGroup::builder()
                            .group_id(GroupId(to_kafka_str(&state.configuration.group_id()?)))
                            .topics(
                                Some(
                                    state.connected_topics
                                        .iter()
                                        .map(|topic_name| 
                                            state.broker_metadata.topics
                                                .get(topic_name)
                                                .unwrap()
                                                .clone()
                                        )
                                        .map(|topic| 
                                            OffsetFetchRequestTopics::builder()
                                                .name(TopicName(to_kafka_str(&topic.name)))
                                                .partition_indexes(
                                                    topic.partitions
                                                        .keys()
                                                        .copied()
                                                        .collect()
                                                )
                                                .build()
                                                .unwrap()
                                        )
                                        .collect()
                                )
                            )
                            .build()?
                    )
                )
                .build()?
        )
    }
}

impl ProcessResponse<OffsetFetchResponse> for OffsetFetchResponse {
    fn process_response(&self, state: &mut crate::io::call_state::CallState) -> Result<(), Box<dyn std::error::Error>> {
        if self.error_code != 0 {
            return Err(Box::new(KafkaCallerError::new(&format!("OffsetFetch response returned error code: '{}'", self.error_code))));
        };

        self.groups
            .iter()
            .map(|group| -> &Vec<OffsetFetchResponseTopics> {&group.topics})
            .for_each(|topics| {
                topics
                    .iter()
                    .for_each(|topic| {
                        state.fetch_state
                            .entry(topic.name.to_string()).or_insert(
                                topic.partitions
                                .iter()
                                .map(|partition| -> (i32, PartitionOffsetState) {
                                    (partition.partition_index, PartitionOffsetState::new(partition.committed_offset))
                                })
                                .collect::<HashMap<i32, PartitionOffsetState>>()
                            );
                    });
            });

        Ok(())
    }
}