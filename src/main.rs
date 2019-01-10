#![allow(dead_code)]
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;

mod extract;
mod find_indices;
mod indices;
mod page;
mod redirect;
mod settings;
mod template;
mod utils;

use std::fs::File;
use std::io::{self, BufWriter};
use std::path::Path;
use std::sync::Mutex;

use tantivy::{
    collector::TopCollector, directory::MmapDirectory, query::QueryParser, schema::*, Index,
};

use crate::extract::{index_anchors, extract_with_writer, extract_anchor_counts};
use crate::indices::{read_indices, write_all_indices, write_template_indices, WikiDumpIndices};
use crate::page::writer::{AnchorWriterJSONL, AnchorWriterTSV};
use crate::redirect::write_redirects;
use crate::settings::Settings;
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

/// Try to create a new BufWriter with the given buffer size wrapped in a mutex.
///
/// # Arguments
/// * `out_path` - Output path
/// * `buf_size` - Buffer size for BufWriter
fn mutex_bufwriter<P: AsRef<Path>>(
    out_path: P,
    buf_size: usize,
) -> io::Result<Mutex<BufWriter<File>>> {
    let writer = File::create(out_path)?;
    let writer = if buf_size == 0 {
        BufWriter::new(writer)
    } else {
        BufWriter::with_capacity(buf_size, writer)
    };
    Ok(Mutex::new(writer))
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

    let anchors = &settings.anchors;
    if !anchors.anchors.exists() {
        write_anchors(&page_indices, &data.dump, &anchors.anchors, 4096)
            .expect("Failed to extract anchors!");
    }


    let anchor_counts = extract_anchor_counts(&page_indices, &data.dump);
    let out_file = File::open("anchor-counts-2019-01-10").unwrap();
    let mut writer = BufWriter::with_capacity(4096, out_file);
    let mut prog_bar = pbr::ProgressBar::new(anchor_counts.len() as u64);
    use std::io::Write;
    for (surface_form, entities) in anchor_counts {
        for (entity, count) in entities {
            writeln!(writer, "{}\t{}\t{}", surface_form, entity, count).unwrap();
        }
        prog_bar.inc();
    }
    
    /*
    let index_dir = &settings.search_index.index_dir;

    let (_schema, _index) = if !index_dir.exists() {
        build_index(index_dir, &page_indices, &data.dump, 500_000_000)
            .expect("Failed to build Index")
    } else {
        let dir = MmapDirectory::open(&index_dir).unwrap();
        let index = Index::open(dir).expect("Failed to load Index");
        (index.schema(), index)
    };
}
