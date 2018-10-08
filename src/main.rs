extern crate regex;

use std::fs::File;
use std::io::{BufReader, BufWriter, prelude::*};
use std::path::Path;
use std::time::SystemTime;
use regex::RegexBuilder;


static TITLE_PATTERN : &'static str = "    <title>";
static REDIRECT_PATTERN : &'static str= "    <redirect";
static TEXT_PATTERN : &'static str = "      <text xml";


fn is_valid_alias(title: &str) -> bool {
    if title.starts_with("Wikipedia:")
        || title.starts_with("Template:")
        || title.starts_with("Portal:")
        || title.starts_with("List of ") {
        return false;
    }
    true
}


fn cleanup_title(title: &str) -> String {
    match title.find("</title>") {
        Some(index) => title[TITLE_PATTERN.len()..index].to_owned(),
        None => title.to_owned(),
    }
}

fn extract<W: Write>(input_path: &str, out: &mut W) -> std::io::Result<()> {
    let redirect_pattern = RegexBuilder::new("#[ ]?[^ ]+[ ]?\\[\\[(.+?)\\]\\]")
                                        .case_insensitive(true)
                                        .build()
                                        .unwrap();
    let start_time = SystemTime::now();
    let input_file = File::open(input_path)?;

    let mut invalid_count = 0;
    let mut count = 0;

    let mut title = String::new();
    let mut in_text = false;
    for line in BufReader::new(input_file).lines() {
        let line = line.unwrap();
        if line.starts_with(TITLE_PATTERN) {
            title = line.clone();
            continue;
        }
        else if line.starts_with(REDIRECT_PATTERN) && (line.starts_with(TEXT_PATTERN) || in_text) {
            match redirect_pattern.captures(&line) {
                Some(mch) => {
                    title = cleanup_title(&title);
                    let redirected_title = mch.get(1).unwrap().as_str();
                    if is_valid_alias(&title) {
                        out.write(format!("{}\t{}\n", title, redirected_title).as_bytes())?;
                        count += 1;
                    } else {
                        invalid_count += 1;
                    }
                },
                None => in_text = true,
            }
        }
    }
    println!("---- Wikipedia redirect extraction done ----");
    println!("Discarded {} redirects to wikipedia meta articles.", invalid_count);
    println!("Extracted {} redirects.", count);
    // println!("Saved output: {} ", output_file);
    let duration = start_time.elapsed().unwrap();
    println!("Done in {}.{:0>3}s", duration.as_secs(), duration.subsec_millis());
    Ok(())
}

fn main() {
    if Path::new("redirect.txt").exists() {
        return;
    }
    let out_file = File::create("redirect.txt").unwrap();
    let mut handle = BufWriter::new(&out_file);
    extract("./test_data/sample-jawiki-latest-pages-articles.xml", &mut handle)
        .expect("Failed to extract!");
}
