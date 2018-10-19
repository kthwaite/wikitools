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

use std::path::Path;

use extract_anchors::write_anchors;
use indices::{read_indices, write_all_indices, write_template_indices};
use settings::Settings;
use templates::compile_templates;

fn main() {
    let settings = Settings::new("config.toml").unwrap();

    println!("settings: {:#?}", settings);

    let (data, indices) = (&settings.data, &settings.indices);

    let page_indices = {
        if !indices.pages.exists() {
            write_all_indices(&data.index, &indices.pages);
        }
        read_indices(&indices.pages).unwrap()
    };

    if !indices.templates.exists() {
        write_template_indices(&data.index, &indices.templates);
    }

    if !settings.templates.exists() {
        let template_indices = read_indices(&indices.templates).unwrap();
        compile_templates(&template_indices, &data.dump, &settings.templates);
    };

    let out_path = Path::new("/ebs_large/stores/wikidata/anchors.tsv");
    write_anchors(&page_indices, &data.dump, &out_path);
}
