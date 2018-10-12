use std::fs::File;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::Path;
use std::str;
use std::sync::Mutex;

use pbr;
use quick_xml::{self as qx, events::Event};
use rayon::prelude::*;

use indices::{read_indices};
use utils::open_seek_bzip;


fn is_valid_alias(title: &str) -> bool {
    if title.starts_with("Wikipedia:")
        || title.starts_with("Template:")
        || title.starts_with("Portal:")
        || title.starts_with("List of ") {
        return false;
    }
    true
}


fn extract_xml<R: BufRead, W: Write>(reader: R, writer: &Mutex<BufWriter<W>>) -> io::Result<(usize, usize)> {
    let mut invalid_count = 0;
    let mut count = 0;

    let mut title = String::new();

    let mut buf = Vec::new();
    let mut text_buf = Vec::new();

    let mut reader = qx::Reader::from_reader(reader);
    loop {
        match reader.read_event(&mut buf) {
            Ok(Event::Start(ref tag)) => {
                if let b"title" = tag.name() {
                    title = reader.read_text(b"title", &mut text_buf).unwrap();
                }
            },
            Ok(Event::Empty(ref tag)) => {
                if let b"redirect" = tag.name() {
                    if is_valid_alias(&title) {
                        // TODO: clunky first pass, revise
                        if let Some(to_title) = tag.attributes().filter_map(|a| {
                            if let Ok(attr) = a {
                                if attr.key == b"title" {
                                    return Some(attr);
                                }
                            }
                            None
                        })
                        .map(|a| a.value)
                        .nth(0) {
                            let red = str::from_utf8(&to_title).unwrap();
                            {
                                let mut out = writer.lock().unwrap();
                                writeln!(out, "{}\t{}", title, red)?;
                            }
                            count += 1;
                        }
                    } else {
                        invalid_count += 1;
                    }
                }
            },
            Ok(Event::Eof) => break,
            Ok(_) => (),
            Err(_) => break,
        }
    }
    Ok((count, invalid_count))
}


/// Dump all redirects to file.
pub fn dump_redirects(indices: &Path, data: &Path, out_path: &Path) {
    let idx = read_indices(indices).unwrap();
    let indices = idx.keys().collect::<Vec<_>>();

    let redfile = File::create(out_path).unwrap();
    let redbuf = Mutex::new(BufWriter::with_capacity(1024 * 1024, redfile));

    let pbar = Mutex::new(pbr::ProgressBar::new(indices.len() as u64));

    let (valid, invalid) = indices.into_par_iter()
           .map(|index| {
                let reader = open_seek_bzip(&data, *index).unwrap();
                let (valid, invalid) = extract_xml(reader, &redbuf).unwrap();
                {
                    let mut prog = pbar.lock().unwrap();
                    prog.inc();
                }
                (valid, invalid)
           })
           .reduce(|| (0, 0), |curr, next| {
                (curr.0 + next.0, curr.1 + next.1)
           });
    println!("Dumped {} redirects to {}", valid, out_path.to_str().unwrap());
    println!("{} redirects were invalid", invalid);
}
