// use clap::{App, Arg};

use storage::tantivy::TantivyWikiIndex;
use core::settings::Settings;

fn main() -> Result<(), Box<std::error::Error>> {
    // let app = App::new("query_tantivy")
    //     .version("0.0.0")
    //     .about("Run keyphrase-count queries over a tantivy index of wikipedia data")
    //     .arg(
    //         Arg::with_name("query")
    //             .takes_value(true)
    //             .help("Query to return results for")
    //             .required(false)
    //     )
    //     .arg(
    //         Arg::with_name("outlinks")
    //             .takes_value(true)
    //             .help("Query to return results for")
    //             .required(false)
    //     )
    //     .get_matches();

    // let query = match app.value_of("query") {
    //     Some(query) => query,
    //     None => {
    //         println!("{}", app.usage());
    //         return Ok(());
    //     }
    // };

    let _settings = Settings::new("config.toml")?;
    let index = TantivyWikiIndex::new("./wiki-index-with-links");
    // let count = index.count_matches_for_query(query);
    // println!("Got {} matches", count);
    let count = index.count_mutual_outlinks(&["Nicolas_Poussin"]);
    println!("Got {} matches", count);
    Ok(())
}
