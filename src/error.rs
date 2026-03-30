//! Structured error types for haqlite.
//!
//! Consumers can match on `HaQLiteError` variants to handle different
//! failure modes appropriately.

use std::fmt;

/// Structured error type for haqlite operations.
#[derive(Debug)]
pub enum HaQLiteError {
    /// Write forwarding failed — leader is unreachable or returned an error.
    LeaderUnavailable(String),

    /// Tried to execute a write but this node is not the leader
    /// and no leader address is available for forwarding.
    NotLeader,

    /// SQLite database error (query, prepare, execute, connection).
    DatabaseError(String),

    /// Replication error (WAL sync, LTX apply, walrust failure).
    ReplicationError(String),

    /// Coordinator error (join, leave, handoff, lease).
    CoordinatorError(String),

    /// Engine is closed (semaphore closed, tasks aborted).
    EngineClosed,
}

impl fmt::Display for HaQLiteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LeaderUnavailable(msg) => write!(f, "Leader unavailable: {msg}"),
            Self::NotLeader => write!(f, "Not leader and no leader address available"),
            Self::DatabaseError(msg) => write!(f, "Database error: {msg}"),
            Self::ReplicationError(msg) => write!(f, "Replication error: {msg}"),
            Self::CoordinatorError(msg) => write!(f, "Coordinator error: {msg}"),
            Self::EngineClosed => write!(f, "Engine closed"),
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
    fn test_display_leader_unavailable() {
        let err = HaQLiteError::LeaderUnavailable("connection refused".into());
        assert_eq!(err.to_string(), "Leader unavailable: connection refused");
    }

    #[test]
    fn test_display_not_leader() {
        let err = HaQLiteError::NotLeader;
        assert_eq!(err.to_string(), "Not leader and no leader address available");
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
        let err: Box<dyn std::error::Error> =
            Box::new(HaQLiteError::DatabaseError("test".into()));
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
        let errors = vec![
            HaQLiteError::LeaderUnavailable("test".into()),
            HaQLiteError::NotLeader,
            HaQLiteError::DatabaseError("test".into()),
            HaQLiteError::ReplicationError("test".into()),
            HaQLiteError::CoordinatorError("test".into()),
            HaQLiteError::EngineClosed,
        ];
        for err in errors {
            match err {
                HaQLiteError::LeaderUnavailable(_) => {}
                HaQLiteError::NotLeader => {}
                HaQLiteError::DatabaseError(_) => {}
                HaQLiteError::ReplicationError(_) => {}
                HaQLiteError::CoordinatorError(_) => {}
                HaQLiteError::EngineClosed => {}
            }
        }
    }
}
