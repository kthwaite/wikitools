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

    let schema = schema_builder.build();
    let index = Index::create_in_dir(&index_dir, schema.clone()).unwrap();

    let index_writer = index.writer(buf_size)?;
    let index_writer = Mutex::new(index_writer);

    index_anchors(page_indices, data_dump, &index_writer, &schema);
    index_writer
        .into_inner()
        .expect("Failed to unwrap IndexWriter")
        .commit()
        .unwrap();
    Ok((schema, index))
}

/// Dump a list of redirects to file as tab-separated pairs.
fn dump_redirects(
    page_indices: &WikiDumpIndices,
    data_dump: &Path,
    out_path: &Path,
    buf_size: usize,
) -> io::Result<()> {
    let writer = mutex_bufwriter(out_path, buf_size)?;
    write_redirects(&page_indices, &data_dump, &writer);
    Ok(())
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

/// Build and serialise a FST from flat anchors.
fn build_fst_from_anchors(anchor_counts_flat_path: &Path, output_path: Option<&Path>) -> Result<(), Box<std::error::Error>> {
    let mut timer = Timer::new();

    let output_path = output_path.unwrap_or(Path::new("anchor-counts.fst"));

    info!("Loading anchor counts...");
    timer.reset();
    let file = File::open(anchor_counts_flat_path)?;
    let reader = BufReader::with_capacity(256 * 1024 * 1024, file);
    let anchor_counts: Trie<BString, u32> = bincode::deserialize_from(reader)?;
    timer.finish();
    
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
    let settings = Settings::new("config.toml")?;

    println!("settings: {:#?}", settings);

    let (data, indices) = (&settings.data, &settings.indices);

    // Fetch all page indices, writing to file if they do not already exist.
    let page_indices = {
        if !indices.pages.exists() {
            write_all_indices(&data.index, &indices.pages)?
        } else {
            read_indices(&indices.pages)?
        }
    };

    // Fetch all template indices, writing to file if they do not already exist.
    let template_indices = {
        if !indices.templates.exists() {
            write_template_indices(&data.index, &indices.templates)?
        } else {
            read_indices(&indices.templates)?
        }
    };

    if !settings.templates.exists() {
        compile_templates(&template_indices, &data.dump, &settings.templates);
    };

    let anchors = &settings.anchors;
    if !anchors.anchors.exists() {
        write_anchors(&page_indices, &data.dump, &anchors.anchors, 4096)
            .expect("Failed to extract anchors!");
    }

    if !anchors.anchor_counts.exists() {
        let anchor_counts = extract_anchor_counts_from_anchors("anchors-2019-01-10", None)?;
        let file = File::create("anchor-counts-2019-01-17")?;
        let mut file = BufWriter::with_capacity(8192 * 1024, file);
        write_anchor_counts(anchor_counts, &mut file);
    }
    Ok(())
}
