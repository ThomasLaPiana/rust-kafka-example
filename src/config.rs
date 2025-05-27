pub struct Config {
    pub url: String,
    pub test_payload: String,
    pub test_key: String,
    pub test_topic: String,
}

pub fn get_config() -> Config {
    Config {
        url: "localhost:9092".to_string(),
        test_payload: "test message".to_string(),
        test_key: "test_key".to_string(),
        test_topic: "test_topic".to_string(),
    }
}
