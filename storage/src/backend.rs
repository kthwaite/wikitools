use crate::doc::Doc;

pub trait StorageBackend {
    fn find_by_id(&self, _doc_id: &str) -> Option<Doc> { None }
    fn get_doc(&self, _doc_id: &str) -> Option<Doc> { None }
}