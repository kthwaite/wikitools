use tantivy::{
    directory::MmapDirectory, query::QueryParser, schema::*, Index,
    collector::Count,
};
use clap::{App, Arg};

use wikitools::settings::Settings;

fn count_matches_for_query(index: &Index, schema: &Schema, query: &str) -> usize {
    index.load_searchers().unwrap();

    let searcher = index.searcher();
    /*
    let content = match schema.get_field("content") {
        None => return Err(...),
        Some(content) => content
    };
    */
    let content = schema.get_field("content").unwrap();
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


    let app = App::new("query_tantivy")
        .version("0.0.0")
        .about("Run keyphrase-count queries over a tantivy index of wikipedia data")
        .arg(
            Arg::with_name("query")
                .takes_value(true)
                .help("Query to return results for")
                .required(true)
        )
        .get_matches();

    let query = match app.value_of("query") {
        Some(query) => query,
        None => {
            println!("{}", app.usage());
            return Ok(());
        }
    };

    let schema = create_schema();

    let index = {
        let index_dir = &settings.search_index.index_dir;
        let mmap_dir = MmapDirectory::open(index_dir).unwrap();
        Index::open(mmap_dir).unwrap()
    };
    let count = count_matches_for_query(&index, &schema, &format!(r#""{}""#, query));
    println!("Got {} matches", count);
    Ok(())
}