use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Mutex;

use indices::WikiDumpIndices;
use pbr;
use quick_xml::{
    self as qx,
    events::{Event}
};
use rayon::prelude::*;
use regex::Regex;

use utils::open_seek_bzip;


lazy_static! {
    /// Check if a wikipedia anchor links to an external resource.
    static ref EXT_LINK : Regex = Regex::new("^[A-Za-z]+:").unwrap();
}

/// Wikipedia category label.
#[derive(Debug, Clone)]
pub struct Category(pub String);

impl Category {
    /// Get the fully qualified name of a category.
    pub fn fqn(&self) -> String {
        format!("Category:{}", self.0)
    }
}

/// Wikipedia anchor, representing a link between pages, optionally with a
/// surface realisation.
#[derive(Debug, Clone)]
pub enum Anchor {
    Direct(String),
    Label{
        surface: String,
        page: String
    }
}

impl Anchor {
    /// Parse an anchor string, returning an Anchor.
    pub fn parse(anchor: &str) -> Self {
        match anchor.find("|") {
            Some(index) => {
                let page = anchor[..index].to_owned();
                let surface = anchor[index+1..].trim();
                if surface.is_empty() {
                    Anchor::Direct(page)
                } else {
                    Anchor::Label{page, surface: surface.to_owned()}
                }
            },
            None => Anchor::Direct(anchor.to_owned())
        }
    }

    /// Extract the text of an anchor, given a start index within a string
    /// slice.
    pub fn pare_anchor_match(page: &str, begin: usize) -> Option<&str> {
        let initial = &page[begin + 2..];
        if EXT_LINK.is_match(initial) {
            return None;
        }
        page[begin..].find("]]").and_then(|end| {
            Some(&page[begin + 2..begin + end])
        })
    }

    /// Check if an anchor string points to a file.
    pub fn is_file(anchor: &str) -> bool {
        anchor.starts_with("File:") || anchor.starts_with("Image:")
    }

    /// Check if an anchor points to Wiktionary.
    pub fn is_wiktionary(anchor: &str) -> bool {
        anchor.starts_with("wikt:") || anchor.starts_with("wiktionary:")
    }

    /// Check if an anchor points to Wikisource.
    pub fn is_wikisource(anchor: &str) -> bool {
        anchor.starts_with("s:")
    }

    /// Check if an anchor points to Wikiversity.
    pub fn is_wikiversity(anchor: &str) -> bool {
        anchor.starts_with("v:")
    }

    /// Check if an anchor points to handle.net.
    pub fn is_handle(anchor: &str) -> bool {
        anchor.starts_with("hdl:")
    }
}

/// Collection of Anchors and Categories for a Wikipedia page.
#[derive(Debug, Default, Clone)]
pub struct Page {
    pub title: String,
    pub id: String,
    pub anchors: Vec<Anchor>,
    pub categories: Vec<Category>
}

impl Page {
    /// Create a new Page object, extracting links and categories from the text
    /// of the page.
    pub fn new(title: String, id: String, page: String) -> Self {
        Page {
            title,
            id,
            anchors: Page::extract_anchors(&page),
            categories: Page::extract_categories(&page)
        }
    }

    /// Extract category links from the text of a Wikipedia page, returning a
    /// Vec of Category objects.
    pub fn extract_categories(page: &str) -> Vec<Category> {
        let page = match page.rfind("==References==") {
            Some(index) => &page[index..],
            None => page
        };
        page.match_indices("[[")
            .filter_map(|(begin, _)| {
                let initial = &page[begin + 2..];
                if initial.starts_with("Category:") {
                    return initial.find("]]").and_then(|end| {
                        match initial[..end].find("|") {
                            Some(actual_end) => Some(&initial[9..actual_end]),
                            None => Some(&initial[9..end])
                        }
                    })
                }
                None
            })
            .map(|cat| Category(cat.to_owned()))
            .collect::<Vec<_>>()
    }
    /// Extract links from the text of a Wikipedia page, returning a Vec of
    /// Anchor objects.
    pub fn extract_anchors(page: &str) -> Vec<Anchor> {
        let page = match page.rfind("==References==") {
            Some(index) => &page[..index],
            None => page
        };
        page.match_indices("[[")
            .filter_map(|(begin, _)| Anchor::pare_anchor_match(page, begin))
            .map(Anchor::parse)
            .collect::<Vec<_>>()
    }
}

/// Iterator yielding Page objects for an XML file.
pub struct PageIterator<R: Read> {
    reader: qx::Reader<BufReader<R>>,
    buf: Vec<u8>,
    page_buf: Vec<u8>,
    title: String,
    id: String,
}


impl<R: Read> PageIterator<R> {
    pub fn new(xml_stream: BufReader<R>) -> Self {
        PageIterator {
            reader: qx::Reader::from_reader(xml_stream),
            buf: vec![],
            page_buf: vec![],
            title: String::new(),
            id: String::new()
        }
    }

    fn extract_title(&mut self) {
        self.title = self.reader.read_text(b"title", &mut self.page_buf).unwrap();
    }

    fn extract_id(&mut self) {
        self.id = self.reader.read_text(b"id", &mut self.page_buf).unwrap();
    }
}


impl<R: Read> Iterator for PageIterator<R> {
    type Item = Page;

    fn next(&mut self) -> Option<Self::Item> {
        enum Tag {
            Id,
            Text,
            Title,
            None
        }
        loop {
            let action = {
                match self.reader.read_event(&mut self.buf) {
                    Ok(Event::Start(ref tag)) => {
                        match tag.name() {
                            b"text" => Tag::Text,
                            b"id" => Tag::Id,
                            b"title" => Tag::Title,
                            _ => Tag::None
                        }
                    },
                    Ok(Event::Eof) => break,
                    Ok(_) => Tag::None,
                    Err(_) => break,
                }
            };
            match action {
                Tag::Id => self.extract_id(),
                Tag::Title => self.extract_title(),
                Tag::Text => {
                    match self.reader.read_text(b"text", &mut self.page_buf) {
                        // Don't skip Template: or Portal: for now.
                        Ok(page) => {
                            // Skip over redirects; these are handled separately.
                            if page.starts_with("#redirect")
                            // Skip over files.
                            || page.starts_with("File:")
                            // Skip over Wikipedia internal pages; we ignore these.
                            || page.starts_with("Wikipedia:") {
                                continue;
                            }
                            return Some(Page::new(self.title.clone(), self.id.clone(), page))
                        },
                        Err(_) => return None,
                    }
                }
                _ => ()
            }
        }
        None
    }
}

/// Extract anchors from a Wikipedia dump, sending them to an arbitrary Writer.
pub fn extract_anchors<W: Write + Send + Sync>(indices: &WikiDumpIndices, data: &Path, writer: &Mutex<W>) {
    let mut indices = indices.keys().collect::<Vec<_>>();
    let pbar = Mutex::new(pbr::ProgressBar::new(indices.len() as u64));
    indices.sort();


    indices.into_par_iter()
           .for_each(|index| {
                let store = open_seek_bzip(&data, *index).unwrap();
                let pages = PageIterator::new(store).collect::<Vec<_>>();
                {
                    let mut w = writer.lock().unwrap();
                    pages.into_iter().for_each(|item| {
                        for anchor in item.anchors {
                            match anchor {
                                Anchor::Direct(name) => {
                                    writeln!(&mut w, "{}\t{}\t{}\t{}\t", item.id, item.title, name, name).unwrap();

                                },
                                Anchor::Label{ surface, page } => {
                                    writeln!(&mut w, "{}\t{}\t{}\t{}\t", item.id, item.title, surface, page).unwrap();
                                }
                            }
                        }
                    });
                }
                {
                    let mut bar = pbar.lock().unwrap();
                    bar.inc();
                }
            });
}

/// Write anchors from a Wikipedia dump to file.
pub fn write_anchors(indices: &WikiDumpIndices, dump: &Path, out_path: &Path) -> io::Result<()> {
    let anchor_file = File::create(out_path)?;
    let writer = BufWriter::with_capacity(8192 * 1024, anchor_file);
    let writer = Mutex::new(writer);

    extract_anchors(&indices, &dump, &writer);
    Ok(())
}
