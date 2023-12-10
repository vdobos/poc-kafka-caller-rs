use kafka_protocol::{messages::{HeartbeatRequest, GroupId}, protocol::Builder};

use crate::utils::to_kafka_str;

use super::CreateRequest;

impl CreateRequest<HeartbeatRequest> for HeartbeatRequest {
    fn create_request(&self, state: &crate::io::call_state::CallState) -> Result<HeartbeatRequest, Box<dyn std::error::Error>> {
        Ok(
            HeartbeatRequest::builder()
                .group_id(GroupId(to_kafka_str(&state.configuration.group_id()?)))
                .generation_id(state.group_subscription.generation_id)
                .member_id(to_kafka_str(&state.group_subscription.member_id))
                .build()?
        )
    }
}

// heartbeat response is not preocessed and called by seporate function (if uncommented), so it does not need to impl ProcessResponse