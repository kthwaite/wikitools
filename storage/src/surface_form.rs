use bincode::{deserialize, serialize, Result as BincodeResult, ErrorKind as BincodeError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Record for an individual surface form and associated anchor counts.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SurfaceForm {
    /// Surface form.
    pub text: String,
    /// Map of page titles to counts.
    pub anchors: Vec<(String, f32)>,
    wiki_occurrences: f32,
}

impl SurfaceForm {
    pub fn new(surface_form: &str, anchors: Vec<(String, f32)>) -> Self {
        SurfaceForm::from_string(surface_form.to_string(), anchors)
    }

    pub fn from_string(text: String, anchors: Vec<(String, f32)>) -> Self {
        let wiki_occurrences: f32 = anchors.iter().map(|(_, v)| v).sum();
        SurfaceForm {
            text,
            anchors,
            wiki_occurrences,
        }
    }

    /// Create a surface form record from tab-separated matches in FST search results.
    pub fn from_paired_matches(query: &str, stream: Vec<(String, u64)>) -> Self {
        let anchors = SurfaceForm::paired_matches_to_vec(stream);
        let wiki_occurrences: f32 = anchors.iter().map(|(_, v)| v).sum();
        SurfaceForm {
            text: query.to_string(),
            anchors,
            wiki_occurrences,
        }
    }

    pub fn paired_matches_to_vec(stream: Vec<(String, u64)>) -> Vec<(String, f32)> {
        stream
            .into_iter()
            .map(|(pair, count)| {
                (
                    pair[pair.find('\t').unwrap() + 1..].to_owned(),
                    count as f32,
                )
            })
            .collect()
    }

    /// Get the number of anchors under this surface form.
    pub fn len(&self) -> usize {
        self.anchors.len()
    }

    /// Check if the surface form contains no anchors.
    pub fn is_empty(&self) -> bool {
        self.anchors.is_empty()
    }

    /// Get the total number of occurrences for this surface form.
    pub fn wiki_occurrences(&self) -> f32 {
        self.wiki_occurrences
    }

    /// Add an anchor-count pair, updating the wiki_occurrences value at the same time.
    pub fn add_anchor(&mut self, page: &str, count: usize) {
        self.anchors.push((page.to_string(), count as f32));
        self.wiki_occurrences += count as f32;
    }

    pub fn text_bytes(&self) -> &[u8] {
        self.text.as_bytes()
    }

    /// Deserialise the SurfaceForm from a bytestream using bincode.
    pub fn from_bytes(bytes: &[u8]) -> BincodeResult<Self> {
        deserialize(bytes)
    }

    /// Serialise the SurfaceForm to a bytestream using bincode.
    pub fn to_bytes(&self) -> BincodeResult<Vec<u8>> {
        serialize(self)
    }

    /// Fetch all entity matches.
    pub fn get_all_wiki_matches(&self) -> HashMap<String, f32> {
        self.get_wiki_matches(0.0)
    }

    /// Fetch entity matches above the given commonness threshold.
    pub fn get_wiki_matches(&self, commonness_threshold: f32) -> HashMap<String, f32> {
        // calculate commonness for each entity and filter the ones below the commonness threshold.
        self
            .anchors
            .iter()
            .map(|(text, count)| (text, (*count as f32) / self.wiki_occurrences))
            .filter(|(_text, count)| *count >= commonness_threshold)
            .map(|(text, count)| (text.clone(), count))
            .collect()
        // TODO: calculate commonness for title, title-nv, redirect
    }

    /// Calculate commonness for an entity.
    /// This is determined as:
    ///     (times mention is linked) / (times mention linked to entity)
    ///
    /// Returns zero if the entity is not linked by the mention.
    ///
    pub fn calculate_commonness(&self, en_uri: &str) -> f32 {
        self.anchors
            .iter()
            .find(|(k, _)| k == en_uri)
            .and_then(|(_, v)| Some((*v as f32) / self.wiki_occurrences))
            .unwrap_or(0.0)
    }
}

#[derive(Debug)]
pub enum SurfaceFormStoreError {
    Unknown,
    NoSuchKey,
    SerializeError(Box<BincodeError>),
    Generic(String),
    PutError(String),
    GetError(String),
}
impl std::error::Error for SurfaceFormStoreError {}
impl fmt::Display for SurfaceFormStoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            SurfaceFormStoreError::Generic(err) => write!(f, "Error: {}", err),
            SurfaceFormStoreError::PutError(err) => write!(f, "Put error: {}", err),
            SurfaceFormStoreError::GetError(err) => write!(f, "Get error: {}", err),
            _ => write!(f, "Unknown error"),
        }
    }
}

impl std::convert::From<Box<BincodeError>> for SurfaceFormStoreError {
    fn from(error: Box<BincodeError>) -> Self {
        SurfaceFormStoreError::SerializeError(error)
    }
}

pub trait SurfaceFormStoreRead {
    fn get(&self, surface_form: &str) -> Result<Option<SurfaceForm>, SurfaceFormStoreError>;
}

pub trait SurfaceFormStoreWrite {
    fn put(&mut self, surface_form: &SurfaceForm) -> Result<(), SurfaceFormStoreError>;
    fn put_raw(&mut self, surface_form: &str, anchors: Vec<(String, f32)>) -> Result<(), SurfaceFormStoreError>;
    fn put_many(&mut self, surface_form: Vec<SurfaceForm>) -> Result<(), SurfaceFormStoreError>;
    fn put_many_raw(&mut self, surface_forms: Vec<(String, Vec<(String, f32)>)>) -> Result<(), SurfaceFormStoreError>;
}