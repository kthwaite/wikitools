use crate::backend::{StorageBackend};
use crate::surface_form::SurfaceForm;

use rocksdb::{DB as RocksDB, Error as RocksError};

/// RocksDB storage backend for surface forms lookup.
#[derive(Debug)]
pub struct RocksBackend {
    db: RocksDB,
}

impl RocksBackend {
    /// Open a RocksDB database at the given path.
    pub fn new(path: &str) -> Result<Self, RocksError> {
        let db = RocksDB::open_default(path)?;
        Ok(RocksBackend {
            db
        })
    }
    /// Insert a document into the database.
    pub fn put_doc(&mut self, doc: &SurfaceForm) -> Result<(), RocksError> {
        self.db.put(&doc.id, doc.to_bytes())
    }
}

impl StorageBackend for RocksBackend {
    fn get_doc(&self, sf_id: &str) -> Option<SurfaceForm> {
        match self.db.get(sf_id.as_bytes()) {
            Ok(Some(value)) => {
                Some(SurfaceForm::from_bytes(&value))
            },
            // TODO: handle Err(_)
            _ => None,
        }
    }
}