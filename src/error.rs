//! Structured error types for haqlite.
//!
//! Consumers can match on `HaQLiteError` variants to handle different
//! failure modes appropriately.

use std::fmt;

/// Structured error type for haqlite operations.
#[derive(Debug)]
pub enum HaQLiteError {
    /// Leader returned a client error (4xx). Not retryable; check the SQL or request.
    LeaderClientError { status: u16, body: String },

    /// Leader returned a server error (5xx). May be transient.
    LeaderServerError { status: u16, body: String },

    /// Could not parse the leader's response body. Indicates a protocol mismatch.
    LeaderResponseParseError(String),

    /// Network/connection error reaching the leader after all retries.
    LeaderConnectionError(String),

    /// Tried to execute a write but this node is not the leader
    /// and no leader address is available for forwarding.
    NotLeader,

    /// SQLite database error (query, prepare, execute, connection, lock poisoned).
    DatabaseError(String),

    /// Replication error (WAL sync, LTX apply, walrust failure, manifest publish).
    ReplicationError(String),

    /// Coordinator error (join, leave, handoff, lease).
    CoordinatorError(String),

    /// Engine is closed (semaphore closed, tasks aborted).
    EngineClosed,

    /// Lease contention in Shared mode (could not acquire lease within timeout).
    LeaseContention(String),

    /// Configuration error (missing required stores, invalid settings).
    ConfigurationError(String),
}

impl fmt::Display for HaQLiteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LeaderClientError { status, body } => {
                write!(f, "Leader client error ({status}): {body}")
            }
            Self::LeaderServerError { status, body } => {
                write!(f, "Leader server error ({status}): {body}")
            }
            Self::LeaderResponseParseError(msg) => write!(f, "Leader response parse error: {msg}"),
            Self::LeaderConnectionError(msg) => write!(f, "Leader connection error: {msg}"),
            Self::NotLeader => write!(f, "Not leader and no leader address available"),
            Self::DatabaseError(msg) => write!(f, "Database error: {msg}"),
            Self::ReplicationError(msg) => write!(f, "Replication error: {msg}"),
            Self::CoordinatorError(msg) => write!(f, "Coordinator error: {msg}"),
            Self::EngineClosed => write!(f, "Engine closed"),
            Self::LeaseContention(msg) => write!(f, "Lease contention: {msg}"),
            Self::ConfigurationError(msg) => write!(f, "Configuration error: {msg}"),
        }
    }
}

impl std::error::Error for HaQLiteError {}

/// Convert from anyhow::Error — used internally for transitional compatibility.
impl From<anyhow::Error> for HaQLiteError {
    fn from(err: anyhow::Error) -> Self {
        HaQLiteError::DatabaseError(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, HaQLiteError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_leader_client_error() {
        let err = HaQLiteError::LeaderClientError {
            status: 400,
            body: "bad request".into(),
        };
        assert_eq!(err.to_string(), "Leader client error (400): bad request");
    }

    #[test]
    fn test_display_leader_server_error() {
        let err = HaQLiteError::LeaderServerError {
            status: 500,
            body: "internal".into(),
        };
        assert_eq!(err.to_string(), "Leader server error (500): internal");
    }

    #[test]
    fn test_display_leader_response_parse_error() {
        let err = HaQLiteError::LeaderResponseParseError("invalid json".into());
        assert_eq!(err.to_string(), "Leader response parse error: invalid json");
    }

    #[test]
    fn test_display_leader_connection_error() {
        let err = HaQLiteError::LeaderConnectionError("connection refused".into());
        assert_eq!(
            err.to_string(),
            "Leader connection error: connection refused"
        );
    }

    #[test]
    fn test_display_configuration_error() {
        let err = HaQLiteError::ConfigurationError("missing lease store".into());
        assert_eq!(err.to_string(), "Configuration error: missing lease store");
    }

    #[test]
    fn test_display_not_leader() {
        let err = HaQLiteError::NotLeader;
        assert_eq!(
            err.to_string(),
            "Not leader and no leader address available"
        );
    }

    #[test]
    fn test_display_database_error() {
        let err = HaQLiteError::DatabaseError("syntax error".into());
        assert_eq!(err.to_string(), "Database error: syntax error");
    }

    #[test]
    fn test_display_replication_error() {
        let err = HaQLiteError::ReplicationError("WAL sync failed".into());
        assert_eq!(err.to_string(), "Replication error: WAL sync failed");
    }

    #[test]
    fn test_display_coordinator_error() {
        let err = HaQLiteError::CoordinatorError("lease expired".into());
        assert_eq!(err.to_string(), "Coordinator error: lease expired");
    }

    #[test]
    fn test_display_engine_closed() {
        let err = HaQLiteError::EngineClosed;
        assert_eq!(err.to_string(), "Engine closed");
    }

    #[test]
    fn test_error_trait() {
        let err: Box<dyn std::error::Error> = Box::new(HaQLiteError::DatabaseError("test".into()));
        assert!(err.to_string().contains("test"));
    }

    #[test]
    fn test_from_anyhow() {
        let anyhow_err = anyhow::anyhow!("something broke");
        let haqlite_err: HaQLiteError = anyhow_err.into();
        assert!(matches!(haqlite_err, HaQLiteError::DatabaseError(_)));
        assert!(haqlite_err.to_string().contains("something broke"));
    }

    #[test]
    fn test_result_type_alias() {
        fn returns_ok() -> Result<i32> {
            Ok(42)
        }
        fn returns_err() -> Result<i32> {
            Err(HaQLiteError::NotLeader)
        }
        assert_eq!(returns_ok().unwrap(), 42);
        assert!(returns_err().is_err());
    }

    #[test]
    fn test_match_exhaustive() {
        let errors: Vec<HaQLiteError> = vec![
            HaQLiteError::LeaderClientError {
                status: 400,
                body: "test".into(),
            },
            HaQLiteError::LeaderServerError {
                status: 500,
                body: "test".into(),
            },
            HaQLiteError::LeaderResponseParseError("test".into()),
            HaQLiteError::LeaderConnectionError("test".into()),
            HaQLiteError::NotLeader,
            HaQLiteError::DatabaseError("test".into()),
            HaQLiteError::ReplicationError("test".into()),
            HaQLiteError::CoordinatorError("test".into()),
            HaQLiteError::EngineClosed,
            HaQLiteError::LeaseContention("test".into()),
            HaQLiteError::ConfigurationError("test".into()),
        ];
        for err in errors {
            match err {
                HaQLiteError::LeaderClientError { .. } => {}
                HaQLiteError::LeaderServerError { .. } => {}
                HaQLiteError::LeaderResponseParseError(_) => {}
                HaQLiteError::LeaderConnectionError(_) => {}
                HaQLiteError::NotLeader => {}
                HaQLiteError::DatabaseError(_) => {}
                HaQLiteError::ReplicationError(_) => {}
                HaQLiteError::CoordinatorError(_) => {}
                HaQLiteError::EngineClosed => {}
                HaQLiteError::LeaseContention(_) => {}
                HaQLiteError::ConfigurationError(_) => {}
            }
        }
    }
}
