use env_logger;
use log::{debug, info};
use std::path::Path;
use std::sync::Mutex;
use tantivy::{directory::MmapDirectory, schema::*, Index, IndexWriter};

use storage::page::{PageIterator, TantivyPageIterator};
use storage::tantivy::{index_anchors, TantivyWikiIndex};
use wikitools::loaders::build_or_load_page_indices;
use wikitools::settings::Settings;
use wikitools::utils::open_seek_bzip;
use storage::tokenizer::WikiTitleTokenizer;

fn main() -> Result<(), Box<std::error::Error>> {
    env_logger::init();
    let mut settings = Settings::new("config.toml")?;
    settings.search_index.index_dir = Path::new("./wiki-index-with-links").to_path_buf();

    info!("wikitools dump 0.0.0");
    debug!("settings: {:#?}", settings);

    let indices = build_or_load_page_indices(&settings)?;


    if !settings.search_index.index_dir.exists() {
        info!(
            "Creating search index dir: {}",
            settings.search_index.index_dir.to_str().unwrap()
        );
        std::fs::create_dir(&settings.search_index.index_dir)?;
    }

    let index = {
        info!(
            "Loading search index dir: {}",
            settings.search_index.index_dir.to_str().unwrap()
        );
        TantivyWikiIndex::load_or_create_index(&settings.search_index.index_dir)
    };

    let schema = TantivyWikiIndex::create_schema();

    let index_buf_sz = 1024 * 1024 * 1024;
    let chunk_len = 10_000;

    let index_writer = index.writer(index_buf_sz).unwrap();
    let index_writer = Mutex::new(index_writer);
    let chunk_count = indices.len() / chunk_len;
    info!(
        "Processing {} document chunks in blocks of {}",
        indices.len(),
        chunk_len
    );
    info!("Using index buffer size: {}", index_buf_sz);
    for (index, chunk) in indices
        .keys()
        .collect::<Vec<_>>()
        .chunks(chunk_len)
        .enumerate()
    {
        info!("Processing chunk {}/{}", index, chunk_count);
        index_anchors(chunk.to_vec(), &settings.data.dump, &index_writer, &schema)?;
        let mut writer = index_writer.lock().expect("Failed to unlock indexer");
        info!("Committing pending documents...");
        writer.commit().unwrap();
    }
    Ok(())
}
