use core::{
    indices::{read_indices, write_all_indices, write_template_indices, WikiDumpIndices},
    settings::Settings
};
use log::debug;
use std::io;

pub fn build_or_load_page_indices(settings: &Settings) -> io::Result<WikiDumpIndices> {
    if !settings.indices.pages.exists() {
        debug!("Building page indices");
        write_all_indices(&settings.data.index, &settings.indices.pages)
    } else {
        debug!("Loading page indices from {:?}", settings.indices.pages);
        read_indices(&settings.indices.pages)
    }
}

pub fn build_or_load_template_indices(settings: &Settings) -> io::Result<WikiDumpIndices> {
    if !settings.indices.templates.exists() {
        debug!("Building template indices");
        write_template_indices(&settings.data.index, &settings.indices.templates)
    } else {
        debug!(
            "Loading template indices from {:?}",
            settings.indices.templates
        );
        read_indices(&settings.indices.templates)
    }
}
