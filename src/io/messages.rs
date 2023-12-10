mod api_versions;
mod metadata;
mod find_coordinator;
mod join_group;
mod sync_group;
mod offset_fetch;
mod list_offsets;
pub(in super::super) mod fetch;
mod offset_commit;
mod leave_group;
mod heartbeat;
mod init_producer_id;
mod produce;

use std::cmp::min;
use std::error::Error;
use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{RequestHeader, ResponseHeader, ApiKey};
use kafka_protocol::protocol::{Builder, Decodable, Encodable, HeaderVersion, Message};
use crate::io::call_state::CallState;
use crate::errors::KafkaCallerError;
use crate::utils::to_kafka_str;

pub(in super::super) trait CreateSerDe<Req, Res>
    where
        Req: Encodable + Decodable + Default + Message + HeaderVersion,
        Res: Encodable + Decodable + Default + Message + HeaderVersion
{
    fn new_ser_de(&self, call_state: Option<&CallState>) -> Result<SerDe<Req, Res>, Box<dyn Error>>;
}

impl<Req, Res> CreateSerDe<Req, Res> for ApiKey
    where
        Req: Encodable + Decodable + Default + Message + HeaderVersion,
        Res: Encodable + Decodable + Default + Message + HeaderVersion
{
    fn new_ser_de(&self, call_state: Option<&CallState>) -> Result<SerDe<Req, Res>, Box<dyn Error>> {
        match self {
            ApiKey::ApiVersionsKey => Ok(SerDe::new(*self as i16, None)?),
            ApiKey::MetadataKey | 
            ApiKey::FindCoordinatorKey | 
            ApiKey::JoinGroupKey | 
            ApiKey::SyncGroupKey | 
            ApiKey::HeartbeatKey | 
            ApiKey::OffsetFetchKey | 
            ApiKey::ListOffsetsKey | 
            ApiKey::FetchKey | 
            ApiKey::OffsetCommitKey | 
            ApiKey::LeaveGroupKey | 
            ApiKey::InitProducerIdKey | 
            ApiKey::ProduceKey => Ok(SerDe::new(*self as i16, call_state)?),
            _ => Err(Box::new(KafkaCallerError::new("Unsupported ApiKey")))
        }
    }
}

pub(in super::super) trait CreateRequest<Req>
    where
        Req: Encodable + Decodable + Default + Message + HeaderVersion,
{
    fn create_request(&self, state: &CallState) -> Result<Req, Box<dyn Error>>;
}

pub(in super::super) trait ProcessResponse<Res>
    where
        Res: Encodable + Decodable + Default + Message + HeaderVersion,
{
    fn process_response(&self, state: &mut CallState) -> Result<(), Box<dyn Error>>;
}

#[derive(Debug, Clone)]
pub(in super::super) struct SerDe<Req , Res>
    where
        Req: Encodable + Decodable + Default + Message + HeaderVersion,
        Res: Encodable + Decodable + Default + Message + HeaderVersion
{
    pub(self) _request_message_struct: Req,
    pub(self) _response_mesage_struct: Res,
    api_key: i16,
    used_version: i16,
}

impl<Req, Res> SerDe<Req, Res>
    where
        Req: Encodable + Decodable + Default + Message + HeaderVersion,
        Res: Encodable + Decodable + Default + Message + HeaderVersion
{
    pub fn new(api_key: i16, state_for_version: Option<&CallState>) -> Result<Self, Box<dyn Error>> {
        let used_version = 
            match state_for_version {
                Some(call_state) => {
                    min(
                        Req::VERSIONS.max, 
                    call_state.broker_api_versions
                            .get(&api_key)
                            .ok_or(KafkaCallerError::new(&format!("Could not find broker api version api key '{}'", &api_key)))?
                            .clone()
                            .max()
                            .ok_or(KafkaCallerError::new(&format!("Could not retrieve max value for broker api for api key '{}'", &api_key)))?
                    )
                },
                None => Req::VERSIONS.max
            };
        Ok (
            Self {
                _request_message_struct: Req::default(),
                _response_mesage_struct: Res::default(),
                api_key,
                used_version,
            }
        )
    }

    pub fn serialize(&self, client_id: &str, correlation_id: i32, request_body: Req) -> Result<BytesMut, Box<dyn Error>> {
        let mut bytes = BytesMut::new();

        let request_header =
            RequestHeader::builder()
                .client_id(Some(to_kafka_str(client_id)))
                .request_api_key(self.api_key)
                .correlation_id(correlation_id)
                .request_api_version(self.used_version)
                .build()?;

        let request_header_version = Req::header_version(self.used_version);
        let message_size = (request_header.compute_size(request_header_version)? + request_body.compute_size(self.used_version)?) as i32;

        bytes.put_i32(message_size);
        request_header.encode(&mut bytes, request_header_version)?;
        request_body.encode(&mut bytes, self.used_version)?;

        Ok(bytes)
    }

    pub fn deserialize(&self, response: &mut Bytes) -> Result<(ResponseHeader, Res), Box<dyn Error>> {
        let response_header = ResponseHeader::decode(response, Res::header_version(self.used_version))?;
        let response_body = Res::decode(response, self.used_version)?;

        Ok((response_header, response_body))
    }


}