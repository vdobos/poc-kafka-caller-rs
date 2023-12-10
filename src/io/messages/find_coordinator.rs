use std::error::Error;
use kafka_protocol::messages::{FindCoordinatorRequest, FindCoordinatorResponse};
use kafka_protocol::protocol::Builder;
use crate::errors::KafkaCallerError;
use crate::io::call_state::{CallState, Coordinator};
use crate::io::messages::{CreateRequest, ProcessResponse};
use crate::utils::to_kafka_str;

impl CreateRequest<FindCoordinatorRequest> for FindCoordinatorRequest {
    fn create_request(&self, state: &CallState) -> Result<FindCoordinatorRequest, Box<dyn Error>> {
        Ok(
            FindCoordinatorRequest::builder()
                .coordinator_keys(
                    vec![to_kafka_str(&state.configuration.group_id()?)]
                )
                .build()?
        )
    }
}

impl ProcessResponse<FindCoordinatorResponse> for FindCoordinatorResponse {
    fn process_response(&self, state: &mut CallState) -> Result<(), Box<dyn Error>> {
        if self.error_code != 0 {
            return Err(Box::new(KafkaCallerError::new(&format!("FindCoordinator response returned error code: '{}'", self.error_code))));
        };

        state.coordinators.clear();
        state.coordinators
            .extend(
                self.coordinators
                    .iter()
                    .map(|response_coordinator: &kafka_protocol::messages::find_coordinator_response::Coordinator| -> (String, Coordinator) {
                        let coordinator =
                            Coordinator {
                                group: response_coordinator.key.to_string(),
                                node_id: response_coordinator.node_id.0,
                                host: response_coordinator.host.to_string(),
                                port: response_coordinator.port,
                            };

                        (response_coordinator.key.to_string(), coordinator)
                    })
            );
        Ok(())
    }
}