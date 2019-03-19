use bincode::{serialize, deserialize};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Document
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Doc {
    /// Page title.
    pub id: String,
    /// Map of anchors to counts.
    pub anchor: HashMap<String, usize>,
}

impl Doc {
    pub fn new(id: &str) -> Self {
        Doc {
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

        let doc = Doc::new("Persuasion");
        let bx = doc.to_bytes();
        assert_eq!(bx.len(), 26);
        let doc2 = Doc::from_bytes(&bx);
        assert_eq!(doc2.id, "Persuasion");
    }
}