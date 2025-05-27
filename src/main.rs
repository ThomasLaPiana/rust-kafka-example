mod config;
mod consumer;
mod producer;
mod utils;

#[tokio::main]
async fn main() -> Result<(), Box<dyn ::std::error::Error>> {
    utils::write_to_stdout("Welcome to the Rust + Kafka Demo!\n").await?;

    // Configuration and initialization
    let config = config::get_config();
    let producer = producer::create_producer(&config.url);
    let consumer = consumer::create_consumer(&config.url, &config.test_topic);

    // Publish to Kafka
    producer::send_message(
        producer,
        config.test_topic,
        config.test_payload,
        config.test_key,
    )
    .await?;

    // Consume from Kafka
    consumer::consume_message(consumer).await?;

    Ok(())
}
