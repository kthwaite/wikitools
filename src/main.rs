#![allow(dead_code)]

mod extract;
mod find_indices;
mod indices;
mod page;
mod redirect;
mod settings;
mod surface_forms;
mod template;
mod utils;

use env_logger;
use log::{debug, info};
use std::io;

use crate::extract::extract_anchor_counts_to_trie;
use crate::extract::{TrieBuilderFlat, TrieBuilderNested};
use crate::indices::{read_indices, write_all_indices, write_template_indices, WikiDumpIndices};
use crate::settings::Settings;
use crate::template::compile_templates;
use crate::utils::Timer;

use bincode;
use qp_trie::{
    wrapper::{BStr, BString},
    Trie,
};
use serde::Serialize;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

fn build_or_load_page_indices(settings: &Settings) -> io::Result<WikiDumpIndices> {
    if !settings.indices.pages.exists() {
        info!("Building page indices");
        write_all_indices(&settings.data.index, &settings.indices.pages)
    } else {
        info!("Loading page indices from {:?}", settings.indices.pages);
        read_indices(&settings.indices.pages)
    }
}

fn build_or_load_template_indices(settings: &Settings) -> io::Result<WikiDumpIndices> {
    if !settings.indices.templates.exists() {
        info!("Building template indices");
        write_template_indices(&settings.data.index, &settings.indices.templates)
    } else {
        info!(
            "Loading template indices from {:?}",
            settings.indices.templates
        );
        read_indices(&settings.indices.templates)
    }
}

/// Serialize a Trie into a .qpt binary file.
fn write_to_qpt<V>(
    anchor_counts: &Trie<BString, V>,
    path: &Path,
    buf_size: Option<usize>,
) -> bincode::Result<()>
where
    V: Serialize,
{
    let file = File::create(path)?;
    let buf_size = buf_size.unwrap_or(256 * 1024 * 1024);
    let file = BufWriter::with_capacity(buf_size, file);
    bincode::serialize_into(file, &anchor_counts)
}

fn read_from_qpt<V>(
    anchor_counts_flat_path: &Path,
    buf_size: Option<usize>,
)  -> bincode::Result<Trie<BString, u32>>
where
    V: Serialize,
{
    let mut timer = Timer::new();
    info!("Loading anchor counts...");
    timer.reset();
    let file = File::open(anchor_counts_flat_path)?;
    let buf_size = buf_size.unwrap_or(256 * 1024 * 1024);
    let reader = BufReader::with_capacity(buf_size, file);
    let anchor_counts: Trie<BString, u32> = bincode::deserialize_from(reader)?;
    timer.finish();
    Ok(anchor_counts)
}

/// Build and serialise a FST from flat anchors.
fn build_fst_from_anchors(anchor_counts: Trie<BString, u32>, output_path: Option<&Path>) -> Result<(), Box<std::error::Error>> {
    let mut timer = Timer::new();

    let output_path = output_path.unwrap_or(Path::new("anchor-counts.fst"));
    
    use fst::{Map, MapBuilder, IntoStreamer, Streamer};
    use fst_regex::Regex;

    info!("Stripping anchors...");
    timer.reset();
    let mut anchors = anchor_counts.into_iter()
                .map(|(key, value)| (key.into(), value as u64))
                .collect::<Vec<(String, u64)>>();
    timer.finish();
    info!("Sorting anchors...");
    timer.reset();
    anchors.sort_by(|(k1, _), (k2, _)| k1.partial_cmp(k2).unwrap());
    timer.finish();

    let file = File::create(output_path)?;
    let buf = BufWriter::with_capacity(256 * 1024 * 1024, file);
    let mut bld = MapBuilder::new(buf)?;
    info!("Building FST...");
    timer.reset();
    bld.extend_iter(anchors.into_iter())?;
    bld.finish().unwrap();
    timer.finish();
    Ok(())
}


fn main() -> Result<(), Box<std::error::Error>> {
    env_logger::init();
    let settings = Settings::new("config.toml")?;

    info!("wikitools dump 0.0.0");
    debug!("settings: {:#?}", settings);

    // Fetch all page indices, writing to file if they do not already exist.
    let page_indices = build_or_load_page_indices(&settings)?;

    // Fetch all template indices, writing to file if they do not already exist.
    let template_indices = build_or_load_template_indices(&settings)?;

    // If the templates master file does not exist, create it.
    if !settings.templates.exists() {
        info!("Compiling templates file");
        compile_templates(&template_indices, &settings.data.dump, &settings.templates);
    };


    info!("Building anchor counts...");
    let anchor_counts = extract_anchor_counts_to_trie(
        TrieBuilderFlat,
        &page_indices,
        &settings.data.dump
    );


    if !settings.anchors.anchor_counts.exists() {
        build_fst_from_anchors(anchor_counts, &settings.anchors.anchor_counts)?;
    }

    Ok(())
    }
    Ok(())
}
