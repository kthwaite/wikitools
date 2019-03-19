use std::borrow::Cow;
use std::io::{BufRead, Write};
use std::path::Path;
use std::str;
use std::sync::Mutex;

use pbr;
use quick_xml::{self as qx, events::Event};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::indices::WikiDumpIndices;
use crate::utils::open_seek_bzip;

/// Check if a Wikipedia page title constitutes a valid redirect.
/// Wikipedia internal pages, templates, portals and lists are all currently
/// ignored for the purposes of extracting redirects.
fn is_valid_alias(title: &str) -> bool {
    if title.starts_with("Wikipedia:")
        || title.starts_with("Template:")
        || title.starts_with("Portal:")
        || title.starts_with("List of ")
    {
        return false;
    }
    true
}

/// An individual Wikipedia redirect.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Redirect {
    from: String,
    to: String,
}

/// Iterator over redirects in an XML file of Wikipedia data.
pub struct RedirectIterator<R: BufRead> {
    reader: qx::Reader<R>,
    buf: Vec<u8>,
    text_buf: Vec<u8>,
    title: String,
}

/// Extract the destination page for a <redirect> tag.
fn extract_to<'a>(tag: &'a qx::events::BytesStart) -> Option<Cow<'a, [u8]>> {
    // TODO: clunky first pass, revise
    tag.attributes()
        .filter_map(|a| {
            if let Ok(attr) = a {
                if attr.key == b"title" {
                    return Some(attr);
                }
            }
            None
        })
        .map(|a| a.value)
        .nth(0)
}

impl<R: BufRead> RedirectIterator<R> {
    /// Create a new RedirectIterator from a reader.
    pub fn new(reader: R) -> Self {
        RedirectIterator {
            reader: qx::Reader::from_reader(reader),
            buf: Default::default(),
            text_buf: Default::default(),
            title: Default::default(),
        }
    }
}

impl<R: BufRead> Iterator for RedirectIterator<R> {
    type Item = Redirect;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.reader.read_event(&mut self.buf) {
                Ok(Event::Start(ref tag)) => {
                    if let b"title" = tag.name() {
                        self.title = self.reader.read_text(b"title", &mut self.text_buf).unwrap();
                    }
                }
                Ok(Event::Empty(ref tag)) => {
                    if let b"redirect" = tag.name() {
                        if is_valid_alias(&self.title) {
                            if let Some(to_title) = extract_to(tag) {
                                let to_title = str::from_utf8(&to_title).unwrap();
                                return Some(Redirect {
                                    from: self.title.clone(),
                                    to: to_title.to_owned(),
                                });
                            }
                        }
                    }
                }
                Ok(Event::Eof) => break,
                Ok(_) => (),
                Err(_) => break,
            }
        }
        None
    }
}

/// Dump all redirects to file as tab-separated pairs.
pub fn write_redirects<W: Write + Send + Sync>(
    indices: &WikiDumpIndices,
    data: &Path,
    writer: &Mutex<W>,
) {
    let indices = indices.keys().collect::<Vec<_>>();

    let pbar = Mutex::new(pbr::ProgressBar::new(indices.len() as u64));

    indices.into_par_iter().for_each(|index| {
        let reader = open_seek_bzip(&data, *index).unwrap();
        let iter = RedirectIterator::new(reader);
        let reds = iter.into_iter().collect::<Vec<Redirect>>();
        {
            let mut w = writer.lock().unwrap();
            reds.into_iter().for_each(|red| {
                writeln!(w, "{}\t{}", red.from, red.to).unwrap();
            });
        }
        {
            let mut prog = pbar.lock().unwrap();
            prog.inc();
        }
    });
}
