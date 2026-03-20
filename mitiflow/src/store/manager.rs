//! Store manager — spawns one [`EventStore`] per partition for multi-partition
//! durable workloads.
//!
//! ```rust,no_run
//! use mitiflow::{EventBusConfig, StoreManager};
//!
//! # async fn example() {
//! let session = zenoh::open(zenoh::Config::default()).await.unwrap();
//! let config = EventBusConfig::builder("demo/events".to_string())
//!     .num_partitions(4)
//!     .build()
//!     .unwrap();
//! let tmp = std::env::temp_dir().join("store-mgr");
//! let mut mgr = StoreManager::new(&session, config, &tmp).expect("open stores");
//! mgr.run().await.expect("start stores");
//! // ... run benchmark ...
//! mgr.shutdown_gracefully().await;
//! # }
//! ```

use std::path::Path;

use zenoh::Session;

use super::backend::FjallBackend;
use super::runner::EventStore;
use crate::config::EventBusConfig;
use crate::error::Result;

/// Manages one [`EventStore`] per partition.
///
/// Each partition gets its own [`FjallBackend`] (in a subdirectory named after
/// the partition number) and its own [`EventStore`] instance.
pub struct StoreManager {
    stores: Vec<EventStore>,
}

impl StoreManager {
    /// Open a store per partition under `base_path`.
    ///
    /// Creates directories `{base_path}/0`, `{base_path}/1`, ...
    /// `{base_path}/{num_partitions - 1}` and initialises a [`FjallBackend`]
    /// for each.
    pub fn new(session: &Session, config: EventBusConfig, base_path: &Path) -> Result<Self> {
        let num = config.num_partitions;
        let mut stores = Vec::with_capacity(num as usize);
        for partition in 0..num {
            let dir = base_path.join(partition.to_string());
            let backend = FjallBackend::open(&dir, partition)?;
            let store = EventStore::new(session, backend, config.clone());
            stores.push(store);
        }
        Ok(Self { stores })
    }

    /// Start all stores (calls [`EventStore::run`] on each).
    pub async fn run(&mut self) -> Result<()> {
        for store in &mut self.stores {
            store.run().await?;
        }
        Ok(())
    }

    /// Number of partition stores managed.
    pub fn num_partitions(&self) -> u32 {
        self.stores.len() as u32
    }

    /// Gracefully shut down every store.
    pub async fn shutdown_gracefully(self) {
        for store in self.stores {
            store.shutdown_gracefully().await;
        }
    }
}
