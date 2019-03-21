use tantivy::{
    Index,
    IndexWriter,
    schema::*,
    directory::MmapDirectory,
};
use env_logger;
use log::{info, debug};
use std::sync::Mutex;
use std::path::Path;
use rayon::prelude::*;

use wikitools::loaders::build_or_load_page_indices;
use wikitools::settings::Settings;
use wikitools::page::{PageIterator, TantivyPageIterator};
use wikitools::utils::{open_seek_bzip};


/// Use tantivy to index content for each page.
pub fn index_anchors(
    mut indices: Vec<&usize>,
    data_path: &Path,
    indexer: &Mutex<IndexWriter>,
    schema: &Schema,
) -> Result<(), Box<std::error::Error>> {
    // let mut indices = indices.keys().collect::<Vec<_>>();
    let pbar = Mutex::new(pbr::ProgressBar::new(indices.len() as u64));
    indices.sort();

    let (id, title, content) = (
        schema.get_field("id").unwrap(),
        schema.get_field("title").unwrap(),
        schema.get_field("content").unwrap(),
    );

    indices
        .into_par_iter()
        .map(|index| {
            let store = open_seek_bzip(data_path, *index).unwrap();
            TantivyPageIterator(PageIterator::new(store))
                .map(|(page_id, page_title, page_content)| {
                    let mut doc = Document::default();
                    doc.add_u64(id, page_id.parse::<u64>().unwrap());
                    doc.add_text(title, &page_title);
                    doc.add_text(content, &page_content);
                    doc
                })
                .collect::<Vec<_>>()
        })
        .for_each(|docs| {
            {
                let mut index_writer = indexer.lock().expect("Failed to unlock indexer");
                for doc in docs {
                    index_writer.add_document(doc);
                }
            }
            {
                let mut prog_bar = pbar.lock().expect("Failed to unlock progress-bar");
                prog_bar.inc();
            }
        });
    Ok(())
}


pub fn create_schema() -> Schema {
    let mut schema_builder = SchemaBuilder::default();

    schema_builder.add_u64_field("id", FAST);
    schema_builder.add_text_field("title", STRING | STORED);
    schema_builder.add_text_field("content", TEXT);

    schema_builder.build()
}


fn main() -> Result<(), Box<std::error::Error>> {
    env_logger::init();
    let settings = Settings::new("config.toml")?;

    info!("wikitools dump 0.0.0");
    debug!("settings: {:#?}", settings);

    let indices = build_or_load_page_indices(&settings)?;

    let schema = create_schema();

    if !settings.search_index.index_dir.exists() {
        info!("Creating search index dir: {}", settings.search_index.index_dir.to_str().unwrap());
        std::fs::create_dir(&settings.search_index.index_dir)?;
    }

    let index = {
        info!("Loading search index dir: {}", settings.search_index.index_dir.to_str().unwrap());
        let index_dir = &settings.search_index.index_dir;
        match MmapDirectory::open(index_dir) {
            Ok(mmap_dir) => {
                if Index::exists(&mmap_dir) {
                    Index::open(mmap_dir).unwrap()
                } else {
                    Index::create_in_dir(index_dir, schema.clone()).unwrap()
                }
            }
            _ => Index::create_in_dir(index_dir, schema.clone()).unwrap()
        }
    };

    let index_buf_sz = 1024 * 1024 * 1024;
    let chunk_len = 10_000;

    let index_writer = index.writer(index_buf_sz).unwrap();
    let index_writer = Mutex::new(index_writer);
    let chunk_count = indices.len() / chunk_len;
    info!("Processing {} document chunks in blocks of {}", indices.len(), chunk_len);
    info!("Using index buffer size: {}", index_buf_sz);
    let mut pbar = pbr::ProgressBar::new(chunk_count as u64);
    for (index, chunk) in indices.keys().collect::<Vec<_>>().chunks(chunk_len).enumerate() {
        info!("Processing chunk {}", index);
        index_anchors(chunk.to_vec(), &settings.data.dump, &index_writer, &schema)?;
        let mut writer = index_writer.lock().expect("Failed to unlock indexer");
        info!("Committing pending documents...");
        writer.commit().unwrap();
        pbar.inc();
    }
    Ok(())
}