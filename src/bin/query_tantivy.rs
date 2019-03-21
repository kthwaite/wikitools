use tantivy::{
    directory::MmapDirectory, query::QueryParser, schema::*, Index,
    collector::Count,
};
use env_logger;
use log::{info, debug};

use wikitools::settings::Settings;

fn count_matches_for_query(index: &Index, schema: &Schema, query: &str) -> usize {
    index.load_searchers().unwrap();

    let searcher = index.searcher();
    let (id, title, content) = (
        schema.get_field("id").unwrap(),
        schema.get_field("title").unwrap(),
        schema.get_field("content").unwrap(),
    );
    let query_parser = QueryParser::for_index(&index, vec![content]);
    let query = query_parser.parse_query(query).unwrap();


    searcher.search(&*query, &Count).unwrap() 
}


pub fn create_schema() -> Schema {
    let mut schema_builder = SchemaBuilder::default();

    schema_builder.add_u64_field("id", FAST);
    schema_builder.add_text_field("title", STRING | STORED);
    schema_builder.add_text_field("content", TEXT);

    schema_builder.build()
}

fn main() -> Result<(), Box<std::error::Error>> {
    let settings = Settings::new("config.toml")?;

    info!("query_tantivy 0.0.0");
    debug!("settings: {:#?}", settings);

    let schema = create_schema();

    let index = {
        let index_dir = &settings.search_index.index_dir;
        let mmap_dir = MmapDirectory::open(index_dir).unwrap();
        Index::open(mmap_dir).unwrap()
    };
    let count = count_matches_for_query(&index, &schema, r#""orpheus and eurydice""#);
    println!("Got {} matches", count);
    Ok(())
}