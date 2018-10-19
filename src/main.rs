#[macro_use] extern crate lazy_static;
extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate bzip2;
extern crate config;
extern crate pbr;
extern crate quick_xml;
extern crate rayon;
extern crate regex;
extern crate spinners;
extern crate zip;

pub mod extract_anchors;
pub mod find_indices;
pub mod extract_redirects;
pub mod indices;
pub mod templates;
pub mod utils;

use std::collections::HashMap;
use std::io::{BufRead, BufWriter, Write};
use std::path::{Path};


use indices::{read_indices, write_template_indices};
use templates::compile_templates;
use utils::{open_seek_bzip};

mod settings {
    use std::path::Path;
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
}

use settings::Settings;

fn main() {
    let settings = Settings::new("config.toml").unwrap();
    let data = Path::new(&settings.data.data);
    let index = Path::new(&settings.data.index);

    println!("data: {:?}", data);
    println!("index: {:?}", index);
}
