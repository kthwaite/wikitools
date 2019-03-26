use tagme::TagMeQuery;

use clap::{App, Arg};

use storage::fst::WikiAnchors;
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
                .help("Query to tag entities for")
                .required(true),
        )
        .get_matches();

    let _query = match app.value_of("query") {
        Some(query) => query,
        None => {
            println!("{}", app.usage());
            return Ok(());
        }
    };

    // let map = WikiAnchors::new("./anchors-flat.fst")?;
    use std::path::Path;
    let path = Path::new("./anchor-counts.db");
    let map = RocksDBSurfaceFormStore::new(&path)?;
    let index = TantivyWikiIndex::new("./wiki-index-with-links");

    let mut qry = TagMeQuery::new("The museum is housed in the Louvre Palace, originally built as the Louvre castle in the late 12th to 13th century under Philip II", 1.0);
    let ents = qry.parse(&map, &index);
    println!("before disambiguation: {:?}", ents);
    let ents = qry.disambiguate(&index, &ents);
    println!("========================================\n\n");
    println!("final: {:?}", ents);
    Ok(())
}
