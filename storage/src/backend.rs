use crate::surface_form::SurfaceForm;

pub trait StorageBackend {
    /// Fetch a surface form document from the backend.
    fn get_doc(&self, _sf_id: &str) -> Option<SurfaceForm> { None }
}