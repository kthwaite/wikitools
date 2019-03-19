use std::path::PathBuf;

use config::{Config, ConfigError, File};
use serde::{Deserialize, Serialize};

/// Configuration for Wikipedia data sources.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Data {
    /// Path to the wikipedia dump.
    pub dump: PathBuf,
    /// Path to the wikipedia dump's indices.
    pub index: PathBuf,
}

/// Configuration for Wikipedia data sources.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Indices {
    #[serde(default = "Indices::default_indices_path")]
    pub pages: PathBuf,
    #[serde(default = "Indices::default_template_indices_path")]
    pub templates: PathBuf,
}

impl Indices {
    pub fn default_indices_path() -> PathBuf {
        "indices".into()
    }
    pub fn default_template_indices_path() -> PathBuf {
        "template_indices".into()
    }
}

impl Default for Indices {
    fn default() -> Self {
        Indices {
            pages: Indices::default_indices_path(),
            templates: Indices::default_template_indices_path(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SearchIndex {
    pub index_dir: PathBuf,
}

/// Configuration for anchor summary files.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Anchors {
    #[serde(default = "Anchors::default_anchors_path")]
    pub anchors: PathBuf,
    #[serde(default = "Anchors::default_anchor_counts_path")]
    pub anchor_counts: PathBuf,
}

impl Anchors {
    pub fn default_anchors_path() -> PathBuf {
        "anchors.tsv".into()
    }

    pub fn default_anchor_counts_path() -> PathBuf {
        "anchor_counts.tsv".into()
    }
}

/// Settings aggregate.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Settings {
    pub data: Data,
    #[serde(default)]
    pub indices: Indices,
    #[serde(default = "Settings::default_templates_path")]
    pub templates: PathBuf,
    pub anchors: Anchors,
    pub search_index: SearchIndex,
}

impl Settings {
    pub fn new(path: &str) -> Result<Self, ConfigError> {
        let mut settings = Config::new();
        settings.merge(File::with_name(path))?;
        settings.try_into()
    }

    pub fn default_templates_path() -> PathBuf {
        "templates.xml".into()
    }
}
