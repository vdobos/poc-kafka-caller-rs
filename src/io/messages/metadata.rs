use std::error::Error;
use kafka_protocol::messages::{MetadataRequest, MetadataResponse, TopicName};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::protocol::Builder;
use uuid::Uuid;
use crate::errors::KafkaCallerError;
use crate::io::call_state::{Broker, BrokerMetadata, CallState, Partition, Topic};
use crate::io::messages::{CreateRequest, ProcessResponse};
use crate::utils::to_kafka_str;

impl CreateRequest<MetadataRequest> for MetadataRequest {
    fn create_request(&self, state: &CallState) -> Result<MetadataRequest, Box<dyn Error>> {
        Ok(
            MetadataRequest::builder()
                .topics(
                    Some(
                        state
                            .connected_topics
                                .iter()
                                .map(|topic_name| 
                                    MetadataRequestTopic::builder()
                                        .topic_id(Uuid::nil())
                                        .name(Some(TopicName(to_kafka_str(topic_name))))
                                        .build()
                                        .unwrap()
                                )
                                .collect()
                    )
                )
                .build()?
        )
    }
}

impl ProcessResponse<MetadataResponse> for MetadataResponse {
    fn process_response(&self, state: &mut CallState) -> Result<(), Box<dyn Error>> {
        state.broker_metadata =
            BrokerMetadata {
                cluster_id:
                    self.cluster_id
                        .as_ref()
                        .ok_or(KafkaCallerError::new("No cluster id found when creating broker metadata"))?
                        .to_string(),
                controller_id: self.controller_id.0,
                brokers:
                    self.brokers
                        .iter()
                        .map(|(broker_id, metadata_response_broker)| -> (i32, Broker) {
                            let broker =
                                Broker {
                                    id: broker_id.0,
                                    host: metadata_response_broker.host.to_string(),
                                    port: metadata_response_broker.port,
                                };

                            (broker_id.0, broker)
                        })
                        .collect(),
                topics:
                    self.topics
                        .iter()
                        .map(|(topic_name, metadata_response_topic)| -> (String, Topic) {
                            let topic =
                                Topic {
                                    id: metadata_response_topic.topic_id,
                                    name: topic_name.0.to_string(),
                                    partitions:
                                        metadata_response_topic.partitions
                                            .iter()
                                            .map(|partition_metadata| -> (i32, Partition) {
                                                let partition =
                                                    Partition {
                                                        index: partition_metadata.partition_index,
                                                        leader_id: partition_metadata.leader_id.0,
                                                    };

                                                (partition_metadata.partition_index, partition)
                                            })
                                            .collect(),
                                };

                            (topic_name.to_string(), topic)
                        })
                        .collect(),
            };

        Ok(())
    }
}