pub mod doc;
pub mod backend;
pub mod backend_rocks;

use doc::Doc;
use backend::{StorageBackend};
use backend_rocks::RocksBackend;
use rocksdb::{Error as RocksError};


#[derive(Debug)]
pub struct SurfaceForms {
    /// The actual dicgtionary of surface forms.
    collection: String,
    db: RocksBackend,
}

impl SurfaceForms {
    pub fn new(collection_str: &str) -> Result<Self, RocksError> {
        let collection = collection_str.to_owned();
        Ok(SurfaceForms {
            collection,
            db: RocksBackend::new(collection_str)?
        })
    }
    
    /// Return all information associated with a surface form.
    pub fn get(&self, surface_form: &str) -> Option<Doc> {
        self.db.find_by_id(surface_form)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_storage_retrieve_one_by_id() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("rocks-storage-test");
        let mut backend = RocksBackend::new(path.to_str().unwrap()).unwrap();
        let doc = Doc::new("Persuasion");
        backend.put_doc(&doc).unwrap();
        let doc2 = backend.get_doc("Persuasion").unwrap();
        assert_eq!(doc.id, doc2.id);
    }

    #[test]
    fn test_storage_retrieve_map_contents() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("rocks-storage-test");
        let mut backend = RocksBackend::new(path.to_str().unwrap()).unwrap();
        let mut doc = Doc::new("Persuasion");
        doc.anchor.insert("foo".to_owned(), 1);
        doc.anchor.insert("bar".to_owned(), 2);
        doc.anchor.insert("baz".to_owned(), 4);
        doc.anchor.insert("qux".to_owned(), 3);
        backend.put_doc(&doc).unwrap();
        let doc2 = backend.get_doc("Persuasion").unwrap();
        
        assert_eq!(doc.anchor.get("foo"), doc2.anchor.get("foo"));
        assert_eq!(doc.anchor.get("bar"), doc2.anchor.get("bar"));
        assert_eq!(doc.anchor.get("baz"), doc2.anchor.get("baz"));
        assert_eq!(doc.anchor.get("qux"), doc2.anchor.get("qux"));
    }
}