use std::error::Error;
use bytes::{BufMut, BytesMut, Bytes};
use indexmap::IndexMap;
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::protocol::{Builder, StrBytes, Message, Encodable};
use kafka_protocol::messages::{ConsumerProtocolSubscription, GroupId, JoinGroupRequest, JoinGroupResponse};
use crate::errors::KafkaCallerError;
use crate::io::call_state::{CallState, SubscriptionMember};
use crate::io::messages::{CreateRequest, ProcessResponse};
use crate::utils::{to_kafka_str, to_kafka_strs};

impl CreateRequest<JoinGroupRequest> for JoinGroupRequest {
    fn create_request(&self, state: &CallState) -> Result<JoinGroupRequest, Box<dyn Error>> {
        Ok({
            let mut builder = JoinGroupRequest::builder();

            builder
                .protocol_type(StrBytes::from_str("consumer"))
                .group_id(GroupId(to_kafka_str(&state.configuration.group_id()?)))
                .rebalance_timeout_ms(30500)
                .session_timeout_ms(30000)
                .reason(Some(to_kafka_str("")));

            
            if state.group_subscription.member_id != String::default() {
                builder.member_id(to_kafka_str(&state.group_subscription.member_id));
            }
                
            let subscription =
                ConsumerProtocolSubscription::builder()
                    .topics(to_kafka_strs(&state.connected_topics))
                    .build()?;

            let protocol_metadata_bytes = &mut BytesMut::default();

            // subscription.encode does not add two version bytes (u16) required by kafka broker
            // if this is missing, the JoinGroup api hangs, as broker is unable to deserialize the metadata
            // after a long wait, backend will return response with error 25 and entire group will become unjoinable (at least with the same client id/topics, untested otherwise)
            // as it will hang even on requests with fixed subscription metadata
            protocol_metadata_bytes.put_u16(ConsumerProtocolSubscription::VERSIONS.max as u16);
            subscription.encode(protocol_metadata_bytes, ConsumerProtocolSubscription::VERSIONS.max)?;

            let mut protocols = IndexMap::new();
            protocols.insert(
                to_kafka_str("range"),
                JoinGroupRequestProtocol::builder()
                        .metadata(Bytes::copy_from_slice(protocol_metadata_bytes))
                        .build()?
            );

            builder.protocols(protocols);

            builder.build()?
        })
    }
}

impl ProcessResponse<JoinGroupResponse> for JoinGroupResponse {
    fn process_response(&self, state: &mut CallState) -> Result<(), Box<dyn Error>> {
        match self.error_code {
            79 => {
                state.group_subscription.member_id = self.member_id.to_string();

                return Ok(());
            },
            0 => {},
            _ =>  return Err(Box::new(KafkaCallerError::new(&format!("JoinGroup response returned error code: '{}'", self.error_code)))),
        };

        state.group_subscription.leader_id = self.leader.to_string();
        state.group_subscription.generation_id = self.generation_id;
        
        state.group_subscription.subscriptions.clear();
        state.group_subscription.subscriptions
            .extend(
            self
                        .members
                            .iter()
                            .map(|join_group_member| 
                                SubscriptionMember { 
                                    member_id: join_group_member.member_id.to_string(), 
                                    metadata: join_group_member.metadata.clone(),
                                }
                            )
            );

        Ok(())
    }
}