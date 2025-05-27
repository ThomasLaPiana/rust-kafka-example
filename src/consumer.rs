use rdkafka::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::{Headers, Message};

/// Create a Consumer to pull messages from Kafka
pub fn create_consumer(bootstrap_server: &str, topic: &str) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_server)
        .set("group.id", "test_group")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[topic])
        .expect("Can't subscribe to specified topics!");

    consumer
}

pub async fn consume_message(consumer: StreamConsumer) -> Result<(), Box<dyn ::std::error::Error>> {
    // Read the message from Kafka
    println!("> Retrieving Message...");

    tokio::time::sleep(tokio::time::Duration::from_millis(4000)).await;
    let mut i = 0;
    loop {
        match consumer.recv().await {
            Err(e) => println!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        println!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
                println!(
                    "* Message Received - payload: '{}', topic: {}",
                    payload,
                    m.topic(),
                );
                if let Some(headers) = m.headers() {
                    for header in headers.iter() {
                        println!("  Header {:#?}: {:?}", header.key, header.value);
                    }
                }
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
        i += 1;
        if i >= 10 {
            break;
        }
    }
    Ok(())
}
