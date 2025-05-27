use rdkafka::ClientConfig;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;

use crate::utils;

/// Create a Producer to send messages to Kafka
pub fn create_producer(bootstrap_server: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_server)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Failed to create client")
}

// Publish messages to Kafka
pub async fn send_message(
    producer: FutureProducer,
    topic: String,
    payload: String,
    key: String,
) -> Result<(), Box<dyn ::std::error::Error>> {
    // User Log
    utils::write_to_stdout("> Writing messages to Kafka topic...\n")
        .await
        .unwrap();

    // Spawn a new task to send the message to Kafka
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            producer
                .send_result(FutureRecord::to(&topic).payload(&payload).key(&key))
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            println!("> Message Sent Successfully!");
        }
    });
    Ok(())
}
