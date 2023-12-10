use std::error::Error;
use kafka_protocol::{messages::{LeaveGroupRequest, LeaveGroupResponse, GroupId, leave_group_request::MemberIdentity}, protocol::Builder};

use crate::{io::call_state::CallState, errors::KafkaCallerError, utils::to_kafka_str};

use super::{CreateRequest, ProcessResponse};

impl CreateRequest<LeaveGroupRequest> for LeaveGroupRequest {
    fn create_request(&self, state: &crate::io::call_state::CallState) -> Result<LeaveGroupRequest, Box<dyn std::error::Error>> {

        Ok(
            LeaveGroupRequest::builder()
                .group_id(GroupId(to_kafka_str(&state.configuration.group_id()?)))
                .members(
                    vec!(
                        MemberIdentity::builder()
                            .member_id(to_kafka_str(&state.group_subscription.member_id))
                            .reason(Some(to_kafka_str("the consumer is being closed")))
                            .build()?
                    )                    
                )
                .build()?
        )
    }
}

impl ProcessResponse<LeaveGroupResponse> for LeaveGroupResponse {
    fn process_response(&self, _state: &mut CallState) -> Result<(), Box<dyn Error>> {
        if self.error_code != 0 {
            return Err(Box::new(KafkaCallerError::new(&format!("LeaveGroup response returned error code: '{}'", self.error_code))));
        };

        // LeaveGroup response is not processed atm ...
        Ok(())
    }
}