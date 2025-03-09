use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::{Headers, Message};
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::io::AsyncWriteExt;

async fn write_to_stdout(text: &str) -> Result<(), Box<dyn ::std::error::Error>> {
    let mut stdout = tokio::io::stdout();
    stdout.write(text.as_bytes()).await?;
    stdout.flush().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn ::std::error::Error>> {
    write_to_stdout("Welcome to Kafka Chat!\n").await?;

    // Set up the hardcoded test info
    let url = "localhost:9092";
    let test_payload = "test message";
    let test_key = "test_key";
    let test_topic = "test_topic";

    // Create the Producer and Consumer
    let producer = create_producer(url);
    let consumer = create_consumer(url);
    consumer
        .subscribe(&[test_topic])
        .expect("Can't subscribe to specified topics!");

    // Send the line to Kafka as a producer
    tokio::spawn(async move {
        write_to_stdout("> Writing message to Kafka topic...\n")
            .await
            .unwrap();
        producer
            .send_result(
                FutureRecord::to(test_topic)
                    .payload(test_payload)
                    .key(test_key),
            )
            .unwrap()
            .await
            .unwrap()
            .unwrap();
        println!("> Message Sent Successfully!");
    });

    // Read the message from Kafka
    println!("> Retrieving Message...");

    loop {
        let message = consumer.recv().await.unwrap();
        println!("{:?}", message);
    }
}

/// Create a Producer to send messages to Kafka
fn create_producer(bootstrap_server: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_server)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Failed to create client")
}

/// Create a Consumer to pull messages from Kafka
fn create_consumer(bootstrap_server: &str) -> StreamConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_server)
        .set("group.id", "test_group")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .create()
        .expect("Consumer creation failed")
}
