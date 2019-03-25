use serde::{Deserialize, Serialize};
use bincode::{serialize, deserialize, Result as BincodeResult};
use rocksdb::{DB as RocksDB};
use std::fmt;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct SurfaceForm {
    pub key: String,
    pub anchors: Vec<(String, f32)>
}

impl SurfaceForm {
    pub fn new(surface_form: &str) -> Self {
        SurfaceForm {
            key: surface_form.to_string(),
            anchors: vec![]
        }
    }

    pub fn len(&self) -> usize {
        self.anchors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.anchors.is_empty()
    }

    pub fn add_anchor(&mut self, page: &str, count: usize) {
        self.anchors.push((page.to_string(), count as f32))
    }

    pub fn key_bytes(&self) -> &[u8] {
        self.key.as_bytes()
    }

    pub fn from_bytes(bytes: &[u8]) -> BincodeResult<Self>  {
        deserialize(bytes)
    }

    pub fn to_bytes(&self) -> BincodeResult<Vec<u8>>  {
        serialize(self)
    }
}

#[derive(Clone, Debug)]
enum SurfaceFormError {
    Unknown,
    EncodeError,
    DecodeError,
    NoSuchKey,
    Generic(String),
    PutError(String),
    GetError(String),
}
impl std::error::Error for SurfaceFormError {}
impl fmt::Display for SurfaceFormError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            SurfaceFormError::Generic(err) => write!(f, "Error: {}", err),
            SurfaceFormError::PutError(err) => write!(f, "Put error: {}", err),
            SurfaceFormError::GetError(err) => write!(f, "Get error: {}", err),
            _ => write!(f, "Unknown error"),
        }
    }
}


trait SurfaceFormStore {
    fn get(&self, surface_form: &str) -> Result<Option<SurfaceForm>, SurfaceFormError>;
    fn put(&mut self, surface_form: &SurfaceForm) -> Result<(), SurfaceFormError>;
}

struct RocksDBSurfaceFormStore {
    db: RocksDB
}

impl RocksDBSurfaceFormStore {
    pub fn new(path: &str) -> Result<Self, rocksdb::Error> {
        let db = RocksDB::open_default(path)?;
        Ok(RocksDBSurfaceFormStore {
            db,
        })
    }

}

impl SurfaceFormStore for RocksDBSurfaceFormStore {
    fn get(&self, key: &str) -> Result<Option<SurfaceForm>, SurfaceFormError> {
        let value = match self.db.get(key.as_bytes()) {
            Ok(Some(value)) => value,
            Ok(None) => return Ok(None),
            Err(err) => return Err(SurfaceFormError::GetError(format!("{}", err))),
        };
        match SurfaceForm::from_bytes(&value) {
            Ok(value) => Ok(Some(value)),
            Err(_err) => Err(SurfaceFormError::DecodeError),
        }
    }
    fn put(&mut self, surface_form: &SurfaceForm) -> Result<(), SurfaceFormError> {
        let value = match surface_form.to_bytes() {
            Ok(value) => value,
            Err(_err) => return Err(SurfaceFormError::EncodeError)
        };
        match  self.db.put(surface_form.key_bytes(), value) {
            Ok(()) => Ok(()),
            Err(err) => Err(SurfaceFormError::PutError(format!("{}", err))),
        }
    }
}


#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn serialize_and_deserialize() {
        let v = SurfaceForm::new("foo");
        let vb = v.to_bytes().unwrap();
        let v2 = SurfaceForm::from_bytes(&vb).unwrap();
        assert_eq!(v.key, v2.key)
    }
}