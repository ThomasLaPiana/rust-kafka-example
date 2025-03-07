use std::time::Duration;

use rdkafka::ClientConfig;
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

    let url = "localhost:9092";
    let producer = create_producer(url);

    let test_message = "test message";

    // Send the line to Kafka on the 'chat' topic
    write_to_stdout("Writing message to Kafka topic...\n").await?;
    let result = producer
        .send(
            FutureRecord::to("chat")
                .payload(test_message)
                .key("test_key"),
            Duration::from_secs(0),
        )
        .await;

    // Print the error message if there is one
    let error_text = result.map_err(|err| format!("{:?}", err));
    if let Err(text) = error_text {
        write_to_stdout(&format!(
            "{}{}\n",
            "- !failed writing to Kafka with error: ", text
        ))
        .await?;
    }

    Ok(())
}

/// Create a Producer to send messages to Kafka
fn create_producer(bootstrap_server: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_server)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Failed to create client")
}
