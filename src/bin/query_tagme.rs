use tagme::{TagMe, TagMeParams, TagMeQuery};

use clap::{App, Arg};
use std::fs::read_to_string;
use std::path::Path;

use storage::rocks::RocksDBSurfaceFormStore;
use storage::tantivy::TantivyWikiIndex;

fn main() -> Result<(), Box<std::error::Error>> {
    use env_logger;
    env_logger::init();

    let app = App::new("query_tagme")
        .version("0.0.0")
        .about("Parse and return entities for a text fragment.")
        .arg(
            Arg::with_name("query")
                .takes_value(true)
                .short("q")
                .long("query")
                .help("Query to tag entities for")
                .conflicts_with("file")
                .required(false),
        )
        .arg(
            Arg::with_name("file")
                .takes_value(true)
                .short("f")
                .long("file")
                .help("File to ingest for tagging")
                .conflicts_with("query")
                .required(false),
        )
        .get_matches();


    let query = if let Some(query) = app.value_of("query") {
        query.to_string()
    } else if let Some(path) = app.value_of("file") {
        read_to_string(path)?
    } else {
        println!("{}", app.usage());
        return Ok(());
    };
    println!("Query: {}", query);

    let path = Path::new("./data/anchor-counts.db");
    let map = RocksDBSurfaceFormStore::new(&path)?;
    let index = match TantivyWikiIndex::new("./data/wiki-index") {
        Ok(index) => index,
        Err(err) => {
            println!("{:?}", err);
            return Ok(());
        }
    };
    let tag_me = TagMe::with_params(
        TagMeParams::default()
            //.with_link_probability_threshold(0.001)
            .with_ngram_min(2),
        map,
        index,
    );

    let mut qry = TagMeQuery::new(&query, 0.0);
    let ents = qry.extract_entities(&tag_me);
    println!("========================================\n\n");
    println!("final: {:#?}", ents);
    Ok(())
}
