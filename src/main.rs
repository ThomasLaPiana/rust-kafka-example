use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::producer::FutureProducer;
use schema_registry_converter::async_impl::avro::{AvroDecoder, AvroEncoder};
use schema_registry_converter::async_impl::schema_registry::SrSettings;

mod configuration;
mod consumer;
mod producer;
mod utils;

struct App {
    config: configuration::Config,
    producer: FutureProducer,
    consumer: StreamConsumer,
    encoder: AvroEncoder<'static>,
    decoder: AvroDecoder<'static>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn ::std::error::Error>> {
    utils::write_to_stdout("Welcome to the Rust + Kafka Demo!\n").await?;

    // Configuration and initialization
    let config = configuration::get_config();
    let app = App {
        config: config.clone(),
        producer: producer::create_producer(&config.url),
        consumer: consumer::create_consumer(&config.url, &config.test_topic),
        encoder: AvroEncoder::new(SrSettings::new(config.schema_registry_url.clone())),
        decoder: AvroDecoder::new(SrSettings::new(config.schema_registry_url.clone())),
    };

    // Publish to Kafka
    producer::send_message(
        app.producer,
        app.config.test_topic,
        app.config.test_payload,
        app.config.test_key,
    )
    .await?;

    // Consume from Kafka
    consumer::consume_message(app.consumer).await?;

    Ok(())
}
