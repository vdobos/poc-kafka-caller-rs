use std::error::Error;
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{SyncGroupRequest, SyncGroupResponse, GroupId};
use kafka_protocol::protocol::Builder;
use crate::errors::KafkaCallerError;
use crate::io::call_state::CallState;
use crate::io::messages::{CreateRequest, ProcessResponse};
use crate::utils::to_kafka_str;

impl CreateRequest<SyncGroupRequest> for SyncGroupRequest {
    fn create_request(&self, state: &CallState) -> Result<SyncGroupRequest, Box<dyn Error>> {
        Ok(
          SyncGroupRequest::builder()
            .protocol_name(Some(to_kafka_str("range")))
            .protocol_type(Some(to_kafka_str("consumer")))
            .member_id(to_kafka_str(&state.group_subscription.member_id))
            .group_id(GroupId(to_kafka_str(&state.configuration.group_id()?)))
            .generation_id(state.group_subscription.generation_id)
            .assignments(
                state.group_subscription.subscriptions
                    .iter()
                    .map(|subscription_member| 
                        SyncGroupRequestAssignment::builder()
                            .member_id(to_kafka_str(&subscription_member.member_id))
                            .assignment(subscription_member.metadata.clone())
                            .build()
                            .unwrap()
                    )
                    .collect()
            )
            .build()?
        )
    }
}

impl ProcessResponse<SyncGroupResponse> for SyncGroupResponse {
    fn process_response(&self, _state: &mut CallState) -> Result<(), Box<dyn Error>> {
        if self.error_code != 0 {
            return Err(Box::new(KafkaCallerError::new(&format!("SyncGroup response returned error code: '{}'", self.error_code))));
        };

        // SyncGroup response is not processed atm ...
        Ok(())
    }
}