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

mod extract_anchors;
mod extract_redirects;
mod find_indices;
mod indices;
mod settings;
mod templates;
mod utils;

use std::collections::HashMap;
use std::io::{BufRead, BufWriter, Write};
use std::path::{Path};


use indices::{read_indices, write_all_indices, write_template_indices};
use templates::compile_templates;
use utils::{open_seek_bzip};


use settings::Settings;

fn main() {
    let settings = Settings::new("config.toml").unwrap();

    println!("data: {:?}", settings.data);
    println!("indices: {:?}", settings.indices);

    let (data, indices) = (&settings.data, &settings.indices);

    if !indices.pages.exists() {
        write_all_indices(&data.index, &indices.pages);
    }
    if !indices.templates.exists() {
        write_template_indices(&data.index, &indices.templates);
    }
}
