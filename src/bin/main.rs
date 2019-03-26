#![allow(dead_code, unused_imports)]

use env_logger;
use log::{debug, info};
use std::io;

use core::{
    indices::{
        read_indices, write_all_indices, write_template_indices, WikiDumpIndices,
    },
    settings::Settings,
    utils::Timer,
};
use storage::{
    template::compile_templates,
    rocks::RocksDBSurfaceFormStore
};
use wikitools::extract::{extract_anchor_counts_to_trie, TrieBuilderFlat, TrieBuilderNested};
use wikitools::loaders::{build_or_load_page_indices, build_or_load_template_indices};

use qp_trie::{
    wrapper::{BStr, BString},
    Trie,
};
use serde::Serialize;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

use storage::surface_form::{SurfaceForm, SurfaceFormStoreWrite};
use pbr;
use std::sync::Mutex;
use rayon::prelude::*;

use fst::{IntoStreamer, Map, MapBuilder, Streamer};
use fst_regex::Regex;


fn build_rocksdb_from_anchors<P: AsRef<Path>>(anchor_counts: Trie<BString, Trie<BString, u32>>, path: &P) -> Result<(), Box<std::error::Error>> {
    let mut store = RocksDBSurfaceFormStore::new(&path)?;
    info!("Converting anchor counts...");
    let anchor_counts = anchor_counts.into_iter()
        .map(|(key, values)| (key, values.into_iter().map(|(k, v)| (k.into(), v as f32)).collect::<Vec<(String, f32)>>()))
        .map(|(key, values)| SurfaceForm::from_string(key.into(), values))
        .collect::<Vec<_>>();
    info!("Loading anchor counts into RocksDB backend...");
    store.put_many(anchor_counts)?;
    Ok(())
}

/// Build and serialise a FST from flat anchors.
fn build_fst_from_anchors(
    anchor_counts: Trie<BString, u32>,
    output_path: &Path,
) -> Result<(), Box<std::error::Error>> {
    let mut timer = Timer::new();

    info!("Stripping anchors...");
    timer.reset();
    let mut anchors = anchor_counts
        .into_iter()
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

// use crate::extract::AnchorTrieBuilder;

// fn test_<Builder, V>() -> Result<(), Box<std::error::Error>>
//     where Builder: AnchorTrieBuilder<V> {
//     use std::fs::File;
//     use std::io::BufWriter;
//     use serde_json;
//     let anchor_counts = Builder::extract(Path::new("pages10.xml.bz2"), 0);
//     let file = File::create("anchor-counts-test-sm.json")?;
//     let file = BufWriter::new(file);
//     serde_json::to_writer_pretty(file, &anchor_counts)
// }

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


    if !settings.anchors.anchor_counts.exists() {
        info!("Building anchor counts...");
        // let anchor_counts =
        //     extract_anchor_counts_to_trie(TrieBuilderFlat, &page_indices, &settings.data.dump);
        // info!("Building FST from anchor counts...");
        // build_fst_from_anchors(anchor_counts, &settings.anchors.anchor_counts)?;

        let anchor_counts =
            extract_anchor_counts_to_trie(TrieBuilderNested, &page_indices, &settings.data.dump);
        let path = Path::new("anchor-counts.db");
        build_rocksdb_from_anchors(anchor_counts, &path)?;
    }

    Ok(())
    /*

    for (surface_form, entities) in anchor_counts {
        for (entity, count) in entities {
            writeln!(writer, "{}\t{}\t{}", surface_form, entity, count).unwrap();
        }
        prog_bar.inc();
    }
    let index_dir = &settings.search_index.index_dir;

    let (_schema, _index) = if !index_dir.exists() {
        build_index(index_dir, &page_indices, &data.dump, 500_000_000)
            .expect("Failed to build Index")
    } else {
        let dir = MmapDirectory::open(&index_dir).unwrap();
        let index = Index::open(dir).expect("Failed to load Index");
        (index.schema(), index)
    };

    */
}
