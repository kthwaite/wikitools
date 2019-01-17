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

use std::fs::File;
use std::io::{self, BufWriter};
use std::path::Path;
use std::sync::Mutex;

use tantivy::{
    collector::TopCollector, directory::MmapDirectory, query::QueryParser, schema::*, Index,
};

use crate::extract::{extract_with_writer, index_anchors};
use crate::indices::{read_indices, write_all_indices, write_template_indices, WikiDumpIndices};
use crate::page::writer::{AnchorWriterJSONL, AnchorWriterTSV};
use crate::redirect::write_redirects;
use crate::settings::Settings;
use crate::surface_forms::{extract_anchor_counts_from_anchors, write_anchor_counts};
use crate::template::compile_templates;
use crate::utils::mutex_bufwriter;

/// Build a Tantivy index from anchors in a wikipedia dump.
fn build_index(
    index_dir: &Path,
    page_indices: &WikiDumpIndices,
    data_dump: &Path,
    buf_size: usize,
) -> tantivy::Result<(Schema, Index)> {
    let mut schema_builder = SchemaBuilder::default();
    schema_builder.add_u64_field("id", FAST);
    schema_builder.add_text_field("title", STRING | STORED);
    schema_builder.add_text_field("links", TEXT | STORED);

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

/// Dump page anchors to a JSONL file.
fn dump_page_anchors(
    page_indices: &WikiDumpIndices,
    data_dump: &Path,
    out_path: &Path,
    buf_size: usize,
) -> io::Result<()> {
    let writer = mutex_bufwriter(out_path, buf_size)?;

    extract_with_writer(AnchorWriterJSONL, &page_indices, &data_dump, &writer);
    Ok(())
}

/// Write anchors from a Wikipedia dump to text file.
///
/// # Arguments
/// * `indices` - Parsed Wikipedia page indices for the corresponding dump
/// * `dump` - Path to Wikipedia dump
/// * `out_path` - Output path
/// * `buf_size` - Buffer size for writer
pub fn write_anchors(
    indices: &WikiDumpIndices,
    dump: &Path,
    out_path: &Path,
    buf_size: usize,
) -> io::Result<()> {
    let writer = mutex_bufwriter(out_path, buf_size)?;

    extract_with_writer(AnchorWriterTSV, &indices, &dump, &writer);
    Ok(())
}

fn one_query(index: &Index, schema: &Schema, query: &str) {
    index.load_searchers().unwrap();

    let searcher = index.searcher();
    let (title, links) = (
        schema.get_field("title").unwrap(),
        schema.get_field("links").unwrap(),
    );
    let query_parser = QueryParser::for_index(&index, vec![links]);
    let query = query_parser.parse_query(query).unwrap();

    let mut top_collector = TopCollector::with_limit(10);

    searcher.search(&*query, &mut top_collector).unwrap();
    let doc_addresses = top_collector.docs();

    for doc_address in doc_addresses {
        let retrieved_doc = searcher.doc(doc_address).unwrap();
        if let Some(doc_title) = retrieved_doc.get_first(title) {
            println!("{:?}", doc_title);
        }
    }
}

fn main() -> Result<(), Box<std::error::Error>> {
    let settings = Settings::new("config.toml")?;

    println!("settings: {:#?}", settings);

    let (data, indices) = (&settings.data, &settings.indices);

    let page_indices = {
        if !indices.pages.exists() {
            write_all_indices(&data.index, &indices.pages);
        }
        read_indices(&indices.pages)?
    };

    if !indices.templates.exists() {
        write_template_indices(&data.index, &indices.templates);
    }

    if !settings.templates.exists() {
        let template_indices = read_indices(&indices.templates)?;
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
