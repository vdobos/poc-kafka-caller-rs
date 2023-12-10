use std::error::Error;
use kafka_protocol::{messages::{OffsetCommitRequest, GroupId, offset_commit_request::{OffsetCommitRequestTopic, OffsetCommitRequestPartition}, TopicName, OffsetCommitResponse}, protocol::Builder};

use crate::{utils::to_kafka_str, io::call_state::CallState};

use super::{CreateRequest, ProcessResponse};

 impl CreateRequest<OffsetCommitRequest> for OffsetCommitRequest {
    fn create_request(&self, state: &crate::io::call_state::CallState) -> Result<OffsetCommitRequest, Box<dyn std::error::Error>> {
      Ok(
         OffsetCommitRequest::builder()
            .group_id(GroupId(to_kafka_str(&state.configuration.group_id()?)))
            .member_id(to_kafka_str(&state.group_subscription.member_id))
            // needed for newer API version
            .generation_id_or_member_epoch(state.group_subscription.generation_id)
            //.generation_id(state.group_subscription.generation_id)
            .topics(
               state.fetch_state
                  .iter()
                  .map(|(name, connection_state)| 
                     OffsetCommitRequestTopic::builder()
                        .name(TopicName(to_kafka_str(name)))
                        .partitions(
                           connection_state
                              .iter()
                              .filter(|(_, partition_offset_state)| -> bool {
                                 partition_offset_state.error_code == 0
                              }) 
                              .map(|(index, partition_offset_state)| 
                                 OffsetCommitRequestPartition::builder()
                                    .partition_index(*index)
                                    .committed_offset(partition_offset_state.polled_offset + 1)
                                    .committed_leader_epoch(-1)
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

 impl ProcessResponse<OffsetCommitResponse> for OffsetCommitResponse {
   fn process_response(&self, _state: &mut CallState) -> Result<(), Box<dyn Error>> {
       // OffsetCommit response is not processed atm ...
       Ok(())
   }
}