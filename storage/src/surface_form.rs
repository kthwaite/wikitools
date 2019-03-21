use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Record for an individual surface form and associated anchor counts.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SurfaceForm {
    /// Surface form.
    pub text: String,
    /// Map of page titles to counts.
    pub anchors: HashMap<String, f32>,
    wiki_occurrences: f32,
}

impl SurfaceForm {
    /// Create a surface form record from tab-separated matches in FST search results.
    pub fn from_paired_matches(query: &str, stream: Vec<(String, u64)>) -> Self {
        let anchors = SurfaceForm::paired_matches_to_map(stream);
        let wiki_occurrences: f32 = anchors.values().sum();
        SurfaceForm {
            text: query.to_string(),
            anchors,
            wiki_occurrences,
        }
    }
    pub fn paired_matches_to_map(stream: Vec<(String, u64)>) -> HashMap<String, f32> {
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

    pub fn wiki_occurrences(&self) -> f32 {
        self.wiki_occurrences
    }

    /// Fetch all entity matches.
    pub fn get_all_wiki_matches(&self) -> HashMap<String, f32> {
        self.get_wiki_matches(0.0)
    }

    /// Fetch entity matches above the given commonness threshold.
    pub fn get_wiki_matches(&self, commonness_threshold: f32) -> HashMap<String, f32> {
        // calculate commonness for each entity and filter the ones below the commonness threshold.
        let wiki_matches: HashMap<String, f32> = self
            .anchors
            .iter()
            .map(|(key, count)| (key, (*count as f32) / self.wiki_occurrences))
            .filter(|(_key, count)| *count >= commonness_threshold)
            .map(|(key, count)| (key.clone(), count))
            .collect();
        // TODO: calculate commonness for title, title-nv, redirect
        wiki_matches
    }

    /// Calculate commonness for an entity.
    /// This is determined as:
    ///     (times mention is linked) / (times mention linked to entity)
    ///
    /// Returns zero if the entity is not linked by the mention.
    ///
    pub fn calculate_commonness(&self, en_uri: &str) -> f32 {
        self.anchors
            .get(en_uri)
            .and_then(|v| Some((*v as f32) / self.wiki_occurrences))
            .unwrap_or(0.0)
    }

    pub fn is_empty(&self) -> bool {
        self.anchors.is_empty()
    }
}
