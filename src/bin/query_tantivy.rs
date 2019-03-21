use tantivy::{
    directory::MmapDirectory, query::QueryParser, schema::*, Index,
    collector::Count,
};
use clap::{App, Arg};

use wikitools::settings::Settings;
use storage::tantivy::TantivyWikiIndex;


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

    let index = TantivyWikiIndex::new("./wiki-index");
    let count = index.count_matches_for_query(query);
    println!("Got {} matches", count);
    Ok(())
}