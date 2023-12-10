use kafka_protocol::{messages::{ListOffsetsRequest, list_offsets_request::{ListOffsetsTopic, ListOffsetsPartition}, TopicName, ListOffsetsResponse}, protocol::Builder};

use crate::utils::to_kafka_str;

use super::{CreateRequest, ProcessResponse};

impl CreateRequest<ListOffsetsRequest> for ListOffsetsRequest {
    fn create_request(&self, state: &crate::io::call_state::CallState) -> Result<ListOffsetsRequest, Box<dyn std::error::Error>> {
        Ok(
            ListOffsetsRequest::builder()
                .replica_id(kafka_protocol::messages::BrokerId(-1))
                .isolation_level(0)
                .topics(
                    state.connected_topics
                        .iter()
                        .map(|topic_name| 
                            ListOffsetsTopic::builder()
                                .name(TopicName(to_kafka_str(topic_name)))
                                .partitions(
                                    state.fetch_state
                                        .get(topic_name)
                                        .as_ref()
                                        .unwrap()
                                        .keys()
                                        .copied()
                                        .map(|key|
                                            ListOffsetsPartition::builder()
                                                .partition_index(key)
                                                // this is -2 because it is probably something like "timestamp-2"
                                                // but the way I use this api this is enough, for -1 it will not return proper offset on first call
                                                .timestamp(-2)
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

impl ProcessResponse<ListOffsetsResponse> for ListOffsetsResponse {
    fn process_response(&self, state: &mut crate::io::call_state::CallState) -> Result<(), Box<dyn std::error::Error>> {
        self.topics
            .iter()
            .for_each(|topic_offsets| {
                topic_offsets.partitions
                    .iter()
                    .for_each(|partition| {
                        let fetch_state = 
                            state.fetch_state
                                .get_mut(&topic_offsets.name.to_string())
                                .unwrap()
                                .get_mut(&partition.partition_index)
                                .unwrap();
                            
                        if partition.offset > fetch_state.commited_offset {
                            fetch_state.commited_offset = partition.offset;
                        }
                    });
            });

        Ok(())
    }
}