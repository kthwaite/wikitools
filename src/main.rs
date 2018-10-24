#![allow(dead_code)]
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate serde_derive;
#[macro_use] extern crate serde_json;
extern crate bzip2;
extern crate config;
extern crate fnv;
extern crate pbr;
extern crate quick_xml;
extern crate rayon;
extern crate regex;
extern crate serde;
extern crate spinners;
extern crate tantivy;
extern crate zip;

mod extract_anchors;
mod find_indices;
mod indices;
mod page;
mod settings;
mod redirect;
mod template;
mod utils;

use std::path::Path;
use tantivy::{
    Index,
    schema::*,
    collector::TopCollector,
    query::QueryParser,
    directory::MmapDirectory
};

use extract_anchors::{index_anchors, write_anchors};
use indices::{read_indices, write_all_indices, write_template_indices, WikiDumpIndices};
use settings::Settings;
use template::compile_templates;


/// Build a Tantivy index from anchors in a wikipedia dump.
fn build_index(index_dir: &Path, page_indices: &WikiDumpIndices, data_dump: &Path, buf_size: usize) -> tantivy::Result<(Schema, Index)> {
    let mut schema_builder = SchemaBuilder::default();
    schema_builder.add_u64_field("id", FAST);
    schema_builder.add_text_field("title", STRING | STORED);
    schema_builder.add_text_field("links", TEXT | STORED);

    let schema = schema_builder.build();
    let index = Index::create_in_dir(&index_dir, schema.clone()).unwrap();

    use std::sync::Mutex;
    let index_writer = index.writer(buf_size)?;
    let index_writer = Mutex::new(index_writer);

    index_anchors(page_indices, data_dump, &index_writer, &schema);
    index_writer.into_inner()
                .expect("Failed to unwrap IndexWriter")
                .commit()
                .unwrap();
    Ok((schema, index))
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
        write_anchors(&page_indices, &data.dump, &anchors.anchors)
            .expect("Failed to extract anchors!");
    }


    let index_dir = &settings.search_index.index_dir;

    let (schema, index) = if !index_dir.exists() {
        build_index(index_dir, &page_indices, &data.dump, 500_000_000)
            .expect("Failed to build Index")
    } else {
        let dir = MmapDirectory::open(&index_dir).unwrap();
        let index = Index::open(dir).expect("Failed to open Index");
        (index.schema(), index)
    };

    index.load_searchers().unwrap();

    let searcher = index.searcher();
    let (title, links) = (
        schema.get_field("title").unwrap(),
        schema.get_field("links").unwrap()
    );
    let query_parser = QueryParser::for_index(&index, vec![links]);
    let query = query_parser.parse_query("John Mandeville").unwrap();

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
