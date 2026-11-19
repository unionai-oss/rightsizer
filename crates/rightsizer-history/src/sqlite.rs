//! The sqlite backend (feature `sqlite`, SPEC §8.1): single-user CLI on one
//! machine; refused on network filesystems.
//!
//! **Stub:** opening works (schema creation lands with the backend); the
//! `HistoryStore` implementation arrives next round.

use std::path::Path;
use std::sync::Mutex;

use crate::StoreError;

/// A sqlite-backed store.
pub struct SqliteStore {
    conn: Mutex<rusqlite::Connection>,
}

impl SqliteStore {
    /// Open (or create) the database at `path`.
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        let conn = rusqlite::Connection::open(path).map_err(|e| StoreError::Config(e.to_string()))?;
        Ok(SqliteStore { conn: Mutex::new(conn) })
    }

    /// An in-memory database (tests).
    pub fn in_memory() -> Result<Self, StoreError> {
        let conn = rusqlite::Connection::open_in_memory().map_err(|e| StoreError::Config(e.to_string()))?;
        Ok(SqliteStore { conn: Mutex::new(conn) })
    }

    /// The connection.
    pub fn connection(&self) -> &Mutex<rusqlite::Connection> {
        &self.conn
    }
}
