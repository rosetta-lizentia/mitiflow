//! Persisted per-publisher sequence checkpoint for cross-restart dedup.
//!
//! Uses fjall to durably store the last acknowledged sequence number
//! for each publisher. On subscriber restart, checkpoints are loaded
//! and injected into the [`GapDetector`] so that already-processed
//! events are treated as duplicates.

use std::collections::HashMap;
use std::path::Path;

use crate::error::{Error, Result};
use crate::types::PublisherId;

/// Persists per-publisher sequence checkpoints to disk via fjall.
///
/// Each `ack(pub_id, seq)` durably stores the sequence number so that
/// after a restart, the subscriber can resume from where it left off.
pub struct SequenceCheckpoint {
    #[allow(dead_code)]
    db: fjall::Database,
    keyspace: fjall::Keyspace,
}

impl SequenceCheckpoint {
    /// Open or create a checkpoint store at the given directory path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = fjall::Database::builder(path)
            .open()
            .map_err(|e| Error::CheckpointError(format!("failed to open checkpoint db: {e}")))?;

        let keyspace = db
            .keyspace("checkpoints", fjall::KeyspaceCreateOptions::default)
            .map_err(|e| Error::CheckpointError(format!("failed to open keyspace: {e}")))?;

        Ok(Self { db, keyspace })
    }

    /// Acknowledge (checkpoint) a sequence number for a publisher.
    ///
    /// Only advances the checkpoint — if the stored seq is already >= the
    /// given seq, this is a no-op.
    pub fn ack(&self, pub_id: &PublisherId, seq: u64) -> Result<()> {
        let key = pub_id.to_bytes();
        let current = self.last_checkpoint(pub_id)?;

        if let Some(current_seq) = current {
            if seq <= current_seq {
                return Ok(());
            }
        }

        self.keyspace
            .insert(&key, seq.to_be_bytes())
            .map_err(|e| Error::CheckpointError(format!("insert failed: {e}")))?;

        Ok(())
    }

    /// Get the last checkpointed sequence for a publisher, if any.
    pub fn last_checkpoint(&self, pub_id: &PublisherId) -> Result<Option<u64>> {
        let key = pub_id.to_bytes();
        match self
            .keyspace
            .get(&key)
            .map_err(|e| Error::CheckpointError(format!("get failed: {e}")))?
        {
            Some(bytes) => {
                let arr: [u8; 8] = bytes
                    .as_ref()
                    .try_into()
                    .map_err(|_| Error::CheckpointError("invalid seq bytes".into()))?;
                Ok(Some(u64::from_be_bytes(arr)))
            }
            None => Ok(None),
        }
    }

    /// Load all checkpoints as a map of `PublisherId → last_seq`.
    ///
    /// Used on restart to seed the [`GapDetector::with_checkpoints()`].
    pub fn restore(&self) -> Result<HashMap<PublisherId, u64>> {
        let mut map = HashMap::new();

        for guard in self.keyspace.iter() {
            let kv = guard
                .into_inner()
                .map_err(|e| Error::CheckpointError(format!("iter failed: {e}")))?;
            let key_bytes: &[u8] = kv.0.as_ref();
            let val_bytes: &[u8] = kv.1.as_ref();

            let pub_id = PublisherId::from_bytes(
                key_bytes
                    .try_into()
                    .map_err(|_| Error::CheckpointError("invalid publisher id bytes".into()))?,
            );

            let seq_arr: [u8; 8] = val_bytes
                .try_into()
                .map_err(|_| Error::CheckpointError("invalid seq bytes in checkpoint".into()))?;
            let seq = u64::from_be_bytes(seq_arr);

            map.insert(pub_id, seq);
        }

        Ok(map)
    }

    /// Remove the checkpoint for a specific publisher.
    pub fn remove(&self, pub_id: &PublisherId) -> Result<()> {
        let key = pub_id.to_bytes();
        self.keyspace
            .remove(&key)
            .map_err(|e| Error::CheckpointError(format!("remove failed: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_checkpoint() -> (tempfile::TempDir, SequenceCheckpoint) {
        let dir = tempfile::tempdir().unwrap();
        let cp = SequenceCheckpoint::open(dir.path().join("checkpoints")).unwrap();
        (dir, cp)
    }

    #[test]
    fn ack_and_restore() {
        let (_dir, cp) = temp_checkpoint();
        let pub1 = PublisherId::new();
        let pub2 = PublisherId::new();

        cp.ack(&pub1, 5).unwrap();
        cp.ack(&pub2, 10).unwrap();

        assert_eq!(cp.last_checkpoint(&pub1).unwrap(), Some(5));
        assert_eq!(cp.last_checkpoint(&pub2).unwrap(), Some(10));

        let restored = cp.restore().unwrap();
        assert_eq!(restored.len(), 2);
        assert_eq!(restored[&pub1], 5);
        assert_eq!(restored[&pub2], 10);
    }

    #[test]
    fn ack_only_advances() {
        let (_dir, cp) = temp_checkpoint();
        let pub_id = PublisherId::new();

        cp.ack(&pub_id, 10).unwrap();
        cp.ack(&pub_id, 5).unwrap(); // This should be a no-op.
        cp.ack(&pub_id, 10).unwrap(); // Same seq, no-op.

        assert_eq!(cp.last_checkpoint(&pub_id).unwrap(), Some(10));
    }

    #[test]
    fn ack_advances_forward() {
        let (_dir, cp) = temp_checkpoint();
        let pub_id = PublisherId::new();

        cp.ack(&pub_id, 5).unwrap();
        cp.ack(&pub_id, 15).unwrap();

        assert_eq!(cp.last_checkpoint(&pub_id).unwrap(), Some(15));
    }

    #[test]
    fn unknown_publisher_returns_none() {
        let (_dir, cp) = temp_checkpoint();
        let pub_id = PublisherId::new();
        assert_eq!(cp.last_checkpoint(&pub_id).unwrap(), None);
    }

    #[test]
    fn remove_checkpoint() {
        let (_dir, cp) = temp_checkpoint();
        let pub_id = PublisherId::new();

        cp.ack(&pub_id, 42).unwrap();
        assert_eq!(cp.last_checkpoint(&pub_id).unwrap(), Some(42));

        cp.remove(&pub_id).unwrap();
        assert_eq!(cp.last_checkpoint(&pub_id).unwrap(), None);
    }

    #[test]
    fn persist_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let pub_id = PublisherId::new();

        {
            let cp = SequenceCheckpoint::open(dir.path().join("checkpoints")).unwrap();
            cp.ack(&pub_id, 99).unwrap();
        }

        // Re-open from the same path.
        let cp2 = SequenceCheckpoint::open(dir.path().join("checkpoints")).unwrap();
        assert_eq!(cp2.last_checkpoint(&pub_id).unwrap(), Some(99));

        let restored = cp2.restore().unwrap();
        assert_eq!(restored[&pub_id], 99);
    }
}
