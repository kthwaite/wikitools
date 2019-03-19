use crate::backend::{StorageBackend};
use crate::doc::Doc;

use rocksdb::{DB as RocksDB, Error as RocksError};


#[derive(Debug)]
pub struct RocksBackend {
    db: RocksDB,
}

impl RocksBackend {
    pub fn new(path: &str) -> Result<Self, RocksError> {
        let db = RocksDB::open_default(path)?;
        Ok(RocksBackend {
            db
        })
    }
    pub fn put_doc(&mut self, doc: &Doc) -> Result<(), RocksError> {
        self.db.put(&doc.id, doc.to_bytes())
    }
}

impl StorageBackend for RocksBackend {
    fn get_doc(&self, doc_id: &str) -> Option<Doc> {
        match self.db.get(doc_id.as_bytes()) {
            Ok(Some(value)) => {
                Some(Doc::from_bytes(&value))
            },
            _ => None,
        }
    }
}