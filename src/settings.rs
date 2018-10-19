use config::{Config, ConfigError, File};
use std::path::{PathBuf};

/// Configuration for Wikipedia data sources.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Data {
    pub data: PathBuf,
    pub index: PathBuf,
}

fn default_indices_path() -> PathBuf {
    "indices".into()
}

fn default_template_indices_path() -> PathBuf {
    "template_indices".into()
}

/// Configuration for Wikipedia data sources.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Indices {
    #[serde(default="default_indices_path")]
    pub pages: PathBuf,
    #[serde(default="default_template_indices_path")]
    pub templates: PathBuf,
}

impl Default for Indices {
    fn default() -> Self {
        Indices {
            pages: default_indices_path(),
            templates: default_template_indices_path(),
        }
    }
}


#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Settings {
    pub data: Data,
    #[serde(default)]
    pub indices: Indices,
}

impl Settings {
    pub fn new(path: &str) -> Result<Self, ConfigError> {
        let mut settings = Config::new();
        settings.merge(File::with_name(path))?;
        settings.try_into()
    }
}
