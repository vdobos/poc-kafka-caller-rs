use std::fmt::{Display, Formatter};
use std::error::Error;

#[derive(Debug)]
pub struct KafkaCallerError(pub String);

impl KafkaCallerError {
    pub fn new(description: &str) -> Self {
        Self(String::from(description))
    }
}

impl Display for KafkaCallerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Kafka caller error: {}", self.0)
    }
}

impl Error for KafkaCallerError {
    fn description(&self) -> &str {
        self.0.as_str()
    }

    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}