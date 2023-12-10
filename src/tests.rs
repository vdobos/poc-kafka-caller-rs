#[cfg(test)]
use crate::{Configuration, Consumer, Producer, io::records::PutRecord};

#[tokio::test]
pub async fn test_poll() {
    let configuration = Configuration::ConsumerConfiguration {
        broker_address: String::from("127.0.0.1:9092"),
        client_id: String::from("test-client-rs"),
        group_id: String::from("test-client-rs.group"),
    };

    let mut consumer = Consumer::new(&configuration).await.unwrap();
    consumer.subscribe(vec!("test_topic"));
    let records = consumer.first_poll().await.unwrap();
    println!("{:#?}", records)
}

#[tokio::test]
pub async fn test_put() {
    let configuration = Configuration::ProducerConfiguration {
        broker_address: String::from("127.0.0.1:9092"),
        client_id: String::from("test-client-rs"),
    };

    let mut producer = Producer::new(&configuration).await.unwrap();

    producer
        .put(&mut 
            vec![
                PutRecord::new_with_key_value_str("test_topic", "WOHOO", "It works !"),
                PutRecord::new_with_key_value_str("test_topic", "WOHOO_2", "It works !!"),
                PutRecord::new_with_key_value_str("test_topic", "WOHOO_3", "It works !!")
            ]
        ).await.unwrap();
}