use std::error::Error;
use std::fmt::Debug;
use std::time::Duration;
use errors::KafkaCallerError;
use io::messages::{CreateSerDe, SerDe, CreateRequest, ProcessResponse};
use io::records::{PutRecord, extract_topics};
use kafka_protocol::messages::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, MetadataRequest, MetadataResponse, FindCoordinatorRequest, FindCoordinatorResponse, JoinGroupRequest, JoinGroupResponse, FetchRequest, FetchResponse, SyncGroupRequest, SyncGroupResponse, OffsetFetchRequest, OffsetFetchResponse, ListOffsetsRequest, ListOffsetsResponse, OffsetCommitRequest, OffsetCommitResponse, LeaveGroupRequest, LeaveGroupResponse, HeartbeatRequest, HeartbeatResponse, InitProducerIdRequest, InitProducerIdResponse, ProduceRequest, ProduceResponse};
use kafka_protocol::protocol::{Decodable, Encodable, Message, HeaderVersion};
use kafka_protocol::records::Record;
use tokio::net::TcpStream;
use tokio::task;
use crate::io::call_state::CallState;
use crate::io::IO;
use crate::io::messages::fetch::ProcessFetchResponse;

mod io;
mod errors;
mod utils;
mod tests;

#[derive(Debug, Clone)]
pub enum Configuration {
    ConsumerConfiguration {
        broker_address: String,
        client_id: String,
        group_id: String,
    },
    ProducerConfiguration {
        broker_address: String,
        client_id: String,
    }
}

impl Configuration {
    pub fn client_id(&self) -> String {
        match self {
            Configuration::ProducerConfiguration { broker_address: _, client_id } => client_id.clone(),
            Configuration::ConsumerConfiguration { broker_address: _, client_id, group_id: _ } => client_id.clone()
        }
    }

    pub fn group_id(&self) -> Result<String, KafkaCallerError> {
        match self {
            Configuration::ConsumerConfiguration { broker_address: _, client_id: _, group_id } => Ok(group_id.clone()),
            _ => Err(KafkaCallerError::new("Not supported for producer configuration"))
        }
    }
}

pub struct Consumer {
    state: CallState,
    io: IO,
}

impl Consumer {
    pub async fn new(configuration: &Configuration) -> Result<Self, Box<dyn Error>> {
        if let Configuration::ConsumerConfiguration{broker_address, ..} = configuration {
            let tcp_stream = TcpStream::connect(broker_address).await?;

            Ok(
                Self {
                    state: CallState::new(configuration)?,
                    io: IO::from(tcp_stream),
                }
            )
        } else {
            Err(Box::new(KafkaCallerError::new("Incorrect configuration instance for consumer")))
        }
    }

    pub fn subscribe(&mut self, topics: Vec<&str>) {
        self.state.connected_topics =
            topics
                .iter()
                .map(|&it| -> String {
                    it.to_string()
                })
                .collect();
    }

    // this copies sequence of calls performed by java client when polling for the first time from topic with 1 partition on docker setup 
    // with one broker only. Caller handles poll correctly even when there were offset deletions before first joining, and this method properly
    // polls even on subsequent calls, however these are probably not entirely correct, as it no longer fully matches java client.
    // Theoretically, implementation should handle poll from multiple topics and multiple partitions(on one broker only), but that remains untested.
    // (there are more calls by java client in practice, especially several ApiVersions calls, but this is enough to correctly poll entries)
    pub async fn first_poll(&mut self) -> Result<Vec<Record>, Box<dyn Error>> {
        self.do_call::<ApiVersionsRequest, ApiVersionsResponse>(ApiKey::ApiVersionsKey).await?;
        self.do_call::<MetadataRequest, MetadataResponse>(ApiKey::MetadataKey).await?;
        self.do_call::<FindCoordinatorRequest, FindCoordinatorResponse>(ApiKey::FindCoordinatorKey).await?;
        // first join group returns member id, second performs proper join group
        self.do_call::<JoinGroupRequest, JoinGroupResponse>(ApiKey::JoinGroupKey).await?;
        self.do_call::<JoinGroupRequest, JoinGroupResponse>(ApiKey::JoinGroupKey).await?;
        // sync group is called because otherwise heartbeat returns error
        self.do_call::<SyncGroupRequest, SyncGroupResponse>(ApiKey::SyncGroupKey).await?;
        // heartbeat works and returns error_code 0, so is probably correct for this simple use-case.
        // however it sends static data (with exception of correlation id) to simplify implementation
        // by not having to make part of CallState thread safe
        //self.run_heartbeat().await?;
        self.do_call::<OffsetFetchRequest, OffsetFetchResponse>(ApiKey::OffsetFetchKey).await?;
        self.do_call::<ListOffsetsRequest, ListOffsetsResponse>(ApiKey::ListOffsetsKey).await?;
        let result = self.do_call_fetch().await?;
        if !result.is_empty() {
            self.do_call::<OffsetCommitRequest, OffsetCommitResponse>(ApiKey::OffsetCommitKey).await?;  
        };
        self.do_call::<LeaveGroupRequest, LeaveGroupResponse>(ApiKey::LeaveGroupKey).await?;
 
        Ok(result)
    }

    async fn do_call<Req, Res>(&mut self, api_key: ApiKey) -> Result<(), Box<dyn Error>>
        where
            Req: Debug + Encodable + Decodable + Default + Message + HeaderVersion + CreateRequest<Req>,
            Res: Debug + Encodable + Decodable + Default + Message + HeaderVersion + ProcessResponse<Res>
    {
        let ser_de: SerDe<Req, Res> = api_key.new_ser_de(Some(&self.state))?;

        let request_body = Req::default().create_request(&self.state)?;

        println!("{:#?}", request_body);

        let mut response_bytes = self.io.call(
            ser_de.serialize(
                &self.state.configuration.client_id(), 
                self.state.correlation_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed),  
                request_body
            )?
        ).await?;

        let (_, response_body) = ser_de.deserialize(&mut response_bytes)?;

        println!("{:#?}", response_body);

        response_body.process_response(&mut self.state)?;
        
        Ok(())
    }

    // this is separate because this one returns vector and also response implements different trait
    async fn do_call_fetch(&mut self) -> Result<Vec<Record>, Box<dyn Error>> {
        let ser_de: SerDe<FetchRequest, FetchResponse> = ApiKey::FetchKey.new_ser_de(Some(&self.state))?;

        let request_body = FetchRequest::default().create_request(&self.state)?;

        println!("{:#?}", request_body);

        let mut response_bytes = self.io.call(
            ser_de.serialize(
                &self.state.configuration.client_id(), 
                self.state.correlation_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed),  
                request_body
            )?
        ).await?;

        let (_, response_body) = ser_de.deserialize(&mut response_bytes)?;

        println!("{:#?}", response_body);

        // this comes from different trait than other process_response methods - it returns vector of records besides modifying state
        response_body.process_response(&mut self.state)
    }

    #[allow(dead_code)]
    // this is a super-naive implementation that sends fixed request data to avoid having to make parts of CallState thread-safe
    async fn run_heartbeat(&self) -> Result<(), Box<dyn Error>> {
        let mut heartbeat_io = self.io.with_cloned_arc();
        let ser_de: SerDe<HeartbeatRequest, HeartbeatResponse> = ApiKey::HeartbeatKey.new_ser_de(Some(&self.state)).unwrap();
        let request_body = HeartbeatRequest::default().create_request(&self.state)?;
        let atomic = self.state.correlation_id.clone();
        let client_id = self.state.configuration.client_id().clone();

        let mut heartbeat_interval = tokio::time::interval(Duration::from_millis(5000));

        let heartbeat = task::spawn(async move {
            loop {
                heartbeat_interval.tick().await;
                
                let request_body = request_body.clone();

                println!("{:#?}", request_body);

                let mut response_bytes = 
                    heartbeat_io.call(
                        ser_de.serialize(
                                    &client_id, 
                                    atomic.fetch_add(1, std::sync::atomic::Ordering::Relaxed), 
                                    request_body
                                ).unwrap()
                    ).await.unwrap();

                let (_, response_body) = ser_de.deserialize(&mut response_bytes).unwrap();

                println!("{:#?}", response_body);
            }
        });

        heartbeat.await?;

        Ok(())
    }
} 

pub struct Producer {
    state: CallState,
    io: IO,
}

impl Producer {
    pub async fn new(configuration: &Configuration) -> Result<Self, Box<dyn Error>> {
        if let Configuration::ProducerConfiguration{broker_address, ..} = configuration {
            let tcp_stream = TcpStream::connect(broker_address).await?;

            Ok(
                Self {
                    state: CallState::new(configuration)?,
                    io: IO::from(tcp_stream),
                }
            )
        } else {
            Err(Box::new(KafkaCallerError::new("Incorrect configuration instance for producer")))
        }
    }

    // This copies sequence of calls performed by java client when putting records into topic. for the first time.
    // Implemented and tested to work with one topic with 1 partition on docker setup with one broker only.
    // Put works correctly, even on repeated calls, however this is probably not entirely correct, as it does not fully match java client
    // when performing more than one call. Put theoretically supports multiple topics (each having one partition, and one broker only), but that remains untested.
    // (there are more calls performed by java client in practice, especially several ApiVersions calls, but this is enough to correctly put entries)
    pub async fn put(&mut self, records: &mut Vec<PutRecord>) -> Result<(), Box<dyn Error>> {
        self.state.connected_topics = extract_topics(records);
        self.state.records_to_send.append(records);

        self.do_call::<ApiVersionsRequest, ApiVersionsResponse>(ApiKey::ApiVersionsKey).await?;
        self.do_call::<MetadataRequest, MetadataResponse>(ApiKey::MetadataKey).await?;
        self.do_call::<InitProducerIdRequest, InitProducerIdResponse>(ApiKey::InitProducerIdKey).await?;
        
        let result = self.do_call::<ProduceRequest, ProduceResponse>(ApiKey::ProduceKey).await;
        self.state.records_to_send.clear();
        result
    }

    async fn do_call<Req, Res>(&mut self, api_key: ApiKey) -> Result<(), Box<dyn Error>>
        where
            Req: Debug + Encodable + Decodable + Default + Message + HeaderVersion + CreateRequest<Req>,
            Res: Debug + Encodable + Decodable + Default + Message + HeaderVersion + ProcessResponse<Res>
    {
        let ser_de: SerDe<Req, Res> = api_key.new_ser_de(Some(&self.state))?;

        let request_body = Req::default().create_request(&self.state)?;

        println!("{:#?}", request_body);

        let mut response_bytes = self.io.call(
            ser_de.serialize(
                &self.state.configuration.client_id(), 
                self.state.correlation_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed),  
                request_body
            )?
        ).await?;

        let (_, response_body) = ser_de.deserialize(&mut response_bytes)?;

        println!("{:#?}", response_body);

        response_body.process_response(&mut self.state)?;
        
        Ok(())
    }
}