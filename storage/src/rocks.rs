use pbr;
use rayon::prelude::*;
use rocksdb::{Error as RocksError, WriteBatch, DB as RocksDB};
use std::path::Path;
use std::sync::Mutex;

use super::surface_form::{
    SurfaceForm, SurfaceFormStoreError, SurfaceFormStoreRead, SurfaceFormStoreWrite,
};

impl std::convert::From<RocksError> for SurfaceFormStoreError {
    fn from(error: RocksError) -> Self {
        SurfaceFormStoreError::Generic(error.into_string())
    }
}

#[derive(Debug)]
pub struct RocksDBSurfaceFormStore {
    db: RocksDB,
    chunk_factor: usize,
}

impl RocksDBSurfaceFormStore {
    pub fn new<P: AsRef<Path>>(path: &P) -> Result<Self, rocksdb::Error> {
        let db = RocksDB::open_default(path.as_ref())?;
        Ok(RocksDBSurfaceFormStore {
            db,
            chunk_factor: 20000,
        })
    }
}

impl SurfaceFormStoreRead for RocksDBSurfaceFormStore {
    fn get(&self, text: &str) -> Result<Option<SurfaceForm>, SurfaceFormStoreError> {
        let value = match self.db.get(text.as_bytes()) {
            Ok(Some(value)) => value,
            Ok(None) => return Ok(None),
            Err(err) => return Err(SurfaceFormStoreError::GetError(err.into_string())),
        };
        let value = SurfaceForm::from_bytes(&value)?;
        Ok(Some(value))
    }
}

impl SurfaceFormStoreWrite for RocksDBSurfaceFormStore {
    fn put(&mut self, surface_form: &SurfaceForm) -> Result<(), SurfaceFormStoreError> {
        let value: Vec<u8> = surface_form.to_bytes()?;
        match self.db.put(surface_form.text_bytes(), value) {
            Ok(()) => Ok(()),
            Err(err) => Err(SurfaceFormStoreError::PutError(err.into_string())),
        }
    }

    fn put_raw(
        &mut self,
        surface_form: &str,
        anchors: Vec<(String, f32)>,
    ) -> Result<(), SurfaceFormStoreError> {
        self.put(&SurfaceForm::new(surface_form, anchors))
    }

    fn put_many(&mut self, surface_forms: Vec<SurfaceForm>) -> Result<(), SurfaceFormStoreError> {
        let prog_bar = Mutex::new(pbr::ProgressBar::new(
            (surface_forms.len() / self.chunk_factor) as u64,
        ));
        let lock = Mutex::new(0);
        let result: Result<Vec<_>, _> = surface_forms
            .into_par_iter()
            .chunks(self.chunk_factor)
            .map(|chunk| -> Result<(), SurfaceFormStoreError> {
                let mut batch = WriteBatch::default();
                for surface_form in chunk {
                    let value: Vec<u8> = surface_form.to_bytes()?;
                    batch.put(surface_form.text_bytes(), value)?;
                }
                {
                    let _ = lock.lock().unwrap();
                    self.db.write(batch)?;
                }
                {
                    let mut pbar = prog_bar.lock().unwrap();
                    pbar.inc();
                }
                Ok(())
            })
            .collect();
        result.map(|_| ())
    }

    fn put_many_raw(
        &mut self,
        surface_forms: Vec<(String, Vec<(String, f32)>)>,
    ) -> Result<(), SurfaceFormStoreError> {
        let surface_forms = surface_forms
            .into_iter()
            .map(|(surface_form, anchors)| SurfaceForm::from_string(surface_form, anchors))
            .collect::<Vec<SurfaceForm>>();
        self.put_many(surface_forms)
    }
}


#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn serialize_and_deserialize() {
        let v = SurfaceForm::new("foo", vec![]);
        let vb = v.to_bytes().unwrap();
        let v2 = SurfaceForm::from_bytes(&vb).unwrap();
        assert_eq!(v.text, v2.text)
    }
}
