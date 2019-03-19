pub mod surface_form;
pub mod backend;
pub mod backend_rocks;

use surface_form::SurfaceForm;
use backend::{StorageBackend};
use backend_rocks::RocksBackend;
use rocksdb::{Error as RocksError};


#[derive(Debug)]
pub struct SurfaceForms {
    path: String,
    db: RocksBackend,
}

impl SurfaceForms {
    /// Open the database of surface forms at the given path.
    pub fn new(path: &str) -> Result<Self, RocksError> {
        Ok(SurfaceForms {
            path: path.to_owned(),
            db: RocksBackend::new(path)?
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }
    
    /// Return all information associated with a surface form.
    pub fn get(&self, surface_form: &str) -> Option<SurfaceForm> {
        self.db.get_doc(surface_form)
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
        let doc = SurfaceForm::new("persuasion");
        backend.put_doc(&doc).unwrap();
        let doc2 = backend.get_doc("persuasion").unwrap();
        assert_eq!(doc.id, doc2.id);
    }

    #[test]
    fn test_storage_retrieve_map_contents() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("rocks-storage-test");
        let mut backend = RocksBackend::new(path.to_str().unwrap()).unwrap();
        let mut doc = SurfaceForm::new("persuasion");
        doc.anchor.insert("foo".to_owned(), 1);
        doc.anchor.insert("bar".to_owned(), 2);
        doc.anchor.insert("baz".to_owned(), 4);
        doc.anchor.insert("qux".to_owned(), 3);
        backend.put_doc(&doc).unwrap();
        let doc2 = backend.get_doc("persuasion").unwrap();
        
        assert_eq!(doc.anchor.get("foo"), doc2.anchor.get("foo"));
        assert_eq!(doc.anchor.get("bar"), doc2.anchor.get("bar"));
        assert_eq!(doc.anchor.get("baz"), doc2.anchor.get("baz"));
        assert_eq!(doc.anchor.get("qux"), doc2.anchor.get("qux"));
    }
}