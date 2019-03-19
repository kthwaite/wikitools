use bincode::{serialize, deserialize};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SurfaceForm {
    /// Surface form.
    pub id: String,
    /// Map of page titles to counts.
    pub anchor: HashMap<String, usize>,
}

impl SurfaceForm {
    pub fn new(id: &str) -> Self {
        SurfaceForm {
            id: id.to_owned(),
            anchor: HashMap::default(),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serialize(&self).unwrap()
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        deserialize(bytes).unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_serialise_deserialise() {

        let doc = SurfaceForm::new("Persuasion");
        let bx = doc.to_bytes();
        assert_eq!(bx.len(), 26);
        let doc2 = SurfaceForm::from_bytes(&bx);
        assert_eq!(doc2.id, "Persuasion");
    }
}