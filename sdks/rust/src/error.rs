//! Error types for Kafka operations.

use std::fmt;
use thiserror::Error;

/// Error kind for categorizing Kafka errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    /// Topic not found.
    TopicNotFound,
    /// Partition not found.
    PartitionNotFound,
    /// Message too large.
    MessageTooLarge,
    /// Not the leader for partition.
    NotLeader,
    /// Offset out of range.
    OffsetOutOfRange,
    /// Group coordinator error.
    GroupCoordinator,
    /// Rebalance in progress.
    RebalanceInProgress,
    /// Unauthorized access.
    Unauthorized,
    /// Quota exceeded.
    QuotaExceeded,
    /// Request timeout.
    Timeout,
    /// Connection disconnected.
    Disconnected,
    /// Serialization error.
    Serialization(String),
    /// IO error.
    Io(String),
    /// RPC transport error.
    Transport(String),
    /// Other error.
    Other(String),
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorKind::TopicNotFound => write!(f, "TopicNotFound"),
            ErrorKind::PartitionNotFound => write!(f, "PartitionNotFound"),
            ErrorKind::MessageTooLarge => write!(f, "MessageTooLarge"),
            ErrorKind::NotLeader => write!(f, "NotLeader"),
            ErrorKind::OffsetOutOfRange => write!(f, "OffsetOutOfRange"),
            ErrorKind::GroupCoordinator => write!(f, "GroupCoordinator"),
            ErrorKind::RebalanceInProgress => write!(f, "RebalanceInProgress"),
            ErrorKind::Unauthorized => write!(f, "Unauthorized"),
            ErrorKind::QuotaExceeded => write!(f, "QuotaExceeded"),
            ErrorKind::Timeout => write!(f, "Timeout"),
            ErrorKind::Disconnected => write!(f, "Disconnected"),
            ErrorKind::Serialization(s) => write!(f, "Serialization: {}", s),
            ErrorKind::Io(s) => write!(f, "IO: {}", s),
            ErrorKind::Transport(s) => write!(f, "Transport: {}", s),
            ErrorKind::Other(s) => write!(f, "{}", s),
        }
    }
}

/// Main error type for Kafka operations.
#[derive(Debug, Error)]
#[error("{message}")]
pub struct Error {
    kind: ErrorKind,
    message: String,
    retriable: bool,
}

impl Error {
    /// Create a new error with the given kind and message.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        let retriable = matches!(
            kind,
            ErrorKind::NotLeader
                | ErrorKind::RebalanceInProgress
                | ErrorKind::Timeout
                | ErrorKind::Disconnected
                | ErrorKind::GroupCoordinator
        );
        Self {
            kind,
            message: message.into(),
            retriable,
        }
    }

    /// Get the error kind.
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    /// Check if this error is retriable.
    pub fn is_retriable(&self) -> bool {
        self.retriable
    }

    /// Get the error message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Create a topic not found error.
    pub fn topic_not_found(topic: &str) -> Self {
        Self::new(ErrorKind::TopicNotFound, format!("Topic '{}' not found", topic))
    }

    /// Create a partition not found error.
    pub fn partition_not_found(topic: &str, partition: i32) -> Self {
        Self::new(
            ErrorKind::PartitionNotFound,
            format!("Partition {} not found in topic '{}'", partition, topic),
        )
    }

    /// Create a message too large error.
    pub fn message_too_large(size: usize, max_size: usize) -> Self {
        Self::new(
            ErrorKind::MessageTooLarge,
            format!("Message size {} exceeds maximum {}", size, max_size),
        )
    }

    /// Create a timeout error.
    pub fn timeout(operation: &str) -> Self {
        Self::new(ErrorKind::Timeout, format!("{} timed out", operation))
    }

    /// Create an unauthorized error.
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unauthorized, message)
    }

    /// Create a serialization error.
    pub fn serialization(err: impl std::error::Error) -> Self {
        Self::new(ErrorKind::Serialization(err.to_string()), err.to_string())
    }

    /// Create a transport error.
    pub fn transport(err: impl std::error::Error) -> Self {
        Self::new(ErrorKind::Transport(err.to_string()), err.to_string())
    }

    /// Create a disconnected error.
    pub fn disconnected() -> Self {
        Self::new(ErrorKind::Disconnected, "Connection disconnected")
    }

    /// Create a generic error.
    pub fn other(message: impl Into<String>) -> Self {
        let msg = message.into();
        Self::new(ErrorKind::Other(msg.clone()), msg)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::serialization(err)
    }
}

impl From<rpc_do::RpcError> for Error {
    fn from(err: rpc_do::RpcError) -> Self {
        match err {
            rpc_do::RpcError::Timeout => Error::new(ErrorKind::Timeout, "RPC timeout"),
            rpc_do::RpcError::Unauthorized => {
                Error::new(ErrorKind::Unauthorized, "Unauthorized")
            }
            rpc_do::RpcError::NotFound(msg) => {
                if msg.contains("topic") {
                    Error::new(ErrorKind::TopicNotFound, msg)
                } else {
                    Error::new(ErrorKind::Other(msg.clone()), msg)
                }
            }
            rpc_do::RpcError::Remote { message, .. } => {
                // Parse Kafka-specific errors from message
                if message.contains("UNKNOWN_TOPIC") || message.contains("topic not found") {
                    Error::new(ErrorKind::TopicNotFound, message)
                } else if message.contains("MESSAGE_TOO_LARGE") {
                    Error::new(ErrorKind::MessageTooLarge, message)
                } else if message.contains("NOT_LEADER") {
                    Error::new(ErrorKind::NotLeader, message)
                } else if message.contains("OFFSET_OUT_OF_RANGE") {
                    Error::new(ErrorKind::OffsetOutOfRange, message)
                } else if message.contains("REBALANCE") {
                    Error::new(ErrorKind::RebalanceInProgress, message)
                } else {
                    Error::new(ErrorKind::Other(message.clone()), message)
                }
            }
            _ => Error::transport(err),
        }
    }
}

/// Result type alias for Kafka operations.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_kind_display() {
        assert_eq!(ErrorKind::TopicNotFound.to_string(), "TopicNotFound");
        assert_eq!(ErrorKind::Timeout.to_string(), "Timeout");
        assert_eq!(
            ErrorKind::Serialization("test".to_string()).to_string(),
            "Serialization: test"
        );
    }

    #[test]
    fn test_error_new() {
        let err = Error::new(ErrorKind::TopicNotFound, "test topic");
        assert_eq!(err.kind(), &ErrorKind::TopicNotFound);
        assert_eq!(err.message(), "test topic");
        assert!(!err.is_retriable());
    }

    #[test]
    fn test_retriable_errors() {
        let timeout = Error::timeout("send");
        assert!(timeout.is_retriable());

        let disconnected = Error::disconnected();
        assert!(disconnected.is_retriable());

        let not_found = Error::topic_not_found("test");
        assert!(!not_found.is_retriable());

        let unauthorized = Error::unauthorized("no access");
        assert!(!unauthorized.is_retriable());
    }

    #[test]
    fn test_error_constructors() {
        let err = Error::topic_not_found("orders");
        assert!(err.message().contains("orders"));
        assert_eq!(err.kind(), &ErrorKind::TopicNotFound);

        let err = Error::partition_not_found("orders", 5);
        assert!(err.message().contains("orders"));
        assert!(err.message().contains("5"));

        let err = Error::message_too_large(2000000, 1000000);
        assert!(err.message().contains("2000000"));
        assert!(err.message().contains("1000000"));
    }

    #[test]
    fn test_error_from_serde() {
        let json_err = serde_json::from_str::<i32>("not a number").unwrap_err();
        let err: Error = json_err.into();
        assert!(matches!(err.kind(), ErrorKind::Serialization(_)));
    }

    #[test]
    fn test_error_display() {
        let err = Error::topic_not_found("test-topic");
        let display = format!("{}", err);
        assert!(display.contains("test-topic"));
    }

    #[test]
    fn test_error_other() {
        let err = Error::other("something went wrong");
        assert!(matches!(err.kind(), ErrorKind::Other(_)));
        assert_eq!(err.message(), "something went wrong");
    }
}
