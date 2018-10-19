use config::{Config, ConfigError, File};

/// Configuration for Wikipedia data sources.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Data {
    pub data: String,
    pub index: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Settings {
    pub data: Data
}

impl Settings {
    pub fn new(path: &str) -> Result<Self, ConfigError> {
        let mut settings = Config::new();
        settings.merge(File::with_name(path))?;
        settings.try_into()
    }
}
