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

use storage::tantivy::{index_anchors, create_schema};
use wikitools::loaders::build_or_load_page_indices;
use wikitools::settings::Settings;
use storage::page::{PageIterator, TantivyPageIterator};
use wikitools::utils::{open_seek_bzip};


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
    for (index, chunk) in indices.keys().collect::<Vec<_>>().chunks(chunk_len).enumerate() {
        info!("Processing chunk {}/{}", index, chunk_count);
        index_anchors(chunk.to_vec(), &settings.data.dump, &index_writer, &schema)?;
        let mut writer = index_writer.lock().expect("Failed to unlock indexer");
        info!("Committing pending documents...");
        writer.commit().unwrap();
    }
    Ok(())
}
