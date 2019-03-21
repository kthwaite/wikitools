use fst::{Map, IntoStreamer};
use fst_regex::Regex;
use clap::{App, Arg};
use std::path::Path;
use std::error;


/// Validate path args.
fn is_path(path: String) -> Result<(), String> {
    if Path::new(&path).exists() {
        return Ok(());
    }
    Err(format!("{} is not a valid path", path))
}


/// Fetch the result of one query from the FST.
fn fetch_one(map: &Map, query: &str) -> Result<(), Box<error::Error>> {
    println!("Building regex...");
    let re = Regex::new(&format!("{}\t.*", query))?;
    println!("searching...");
    let stream = map.search(&re).into_stream().into_str_vec()?;
    let stream = stream.iter();
    for (key, count) in stream {
        println!("{}\t{}", key, count);
    }
    Ok(())
}


fn fetch_interactive() -> Result<(), Box<error::Error>> {
    Ok(())
}


fn main() -> Result<(), Box<error::Error>> {
    let app = App::new("query_fst")
        .about("Run queries over a FST")
        .arg(
            Arg::with_name("fst_path").index(1)
                .help("Path to .fst file")
                .validator(is_path)
                .required(true)
        )
        .arg(
            Arg::with_name("query")
                .short("q")
                .long("query")
                .takes_value(true)
                .help("Query to return results for")
                .required(false)
        )
        .get_matches();

    let fst_path = match app.value_of("fst_path") {
        None => {
            println!("{}", app.usage());
            return Ok(());
        },
        Some(fst_path) => fst_path,
    };

    println!("Loading {}", fst_path);
    let map = unsafe { Map::from_path(fst_path) }?;
    match app.value_of("query") {
        Some(query) => {
            fetch_one(&map, query)
        },
        None => fetch_interactive()
    }
}