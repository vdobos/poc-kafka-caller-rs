use kafka_protocol::{messages::{InitProducerIdRequest, InitProducerIdResponse}, protocol::Builder};

use crate::errors::KafkaCallerError;

use super::{CreateRequest, ProcessResponse};

impl CreateRequest<InitProducerIdRequest> for InitProducerIdRequest {
    fn create_request(&self, _state: &crate::io::call_state::CallState) -> Result<InitProducerIdRequest, Box<dyn std::error::Error>> {
        Ok(
            InitProducerIdRequest::builder()
                .transaction_timeout_ms(i32::MAX)
                .transactional_id(None)
                .build()?
        )
    }
}

impl ProcessResponse<InitProducerIdResponse> for InitProducerIdResponse {
    fn process_response(&self, state: &mut crate::io::call_state::CallState) -> Result<(), Box<dyn std::error::Error>> {
        if self.error_code != 0 {
            return Err(Box::new(KafkaCallerError::new(&format!("InitProducerId response returned error code: '{}'", self.error_code))));
        };

        state.producer_id = self.producer_id.0;

        Ok(())
    }
}