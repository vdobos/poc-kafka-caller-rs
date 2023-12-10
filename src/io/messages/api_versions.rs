use std::error::Error;
use std::ops::RangeInclusive;

use kafka_protocol::messages::{ApiVersionsRequest, ApiVersionsResponse};
use kafka_protocol::protocol::Builder;
use crate::errors::KafkaCallerError;

use crate::io::call_state::CallState;
use crate::io::messages::{CreateRequest, ProcessResponse};
use crate::utils::to_kafka_str;

impl CreateRequest<ApiVersionsRequest> for ApiVersionsRequest {
    fn create_request(&self, _state: &CallState) -> Result<ApiVersionsRequest, Box<dyn Error>> {
        Ok(
            ApiVersionsRequest::builder()
                .client_software_name(to_kafka_str("poc-kafka-caller-rs"))
                .client_software_version(to_kafka_str("1.0.0"))
                .build()?
        )
    }
}

impl ProcessResponse<ApiVersionsResponse> for ApiVersionsResponse {
    fn process_response(&self, state: &mut CallState) -> Result<(), Box<dyn Error>> {
        if self.error_code != 0 {
            return Err(Box::new(KafkaCallerError::new(&format!("ApiVersions response returned error code: '{}'", self.error_code))));
        };

        state.broker_api_versions.clear();
        state.broker_api_versions
            .extend(
                self.api_keys
                    .iter()
                    .map(|(index, api_version)| -> (i16, RangeInclusive<i16>) {
                        (*index, api_version.min_version..=api_version.max_version)
                    })
            );

        Ok(())
    }
}