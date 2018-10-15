use quick_xml::{
    self as qx,
    events::{Event}
};
use regex::Regex;
use std::io::{BufReader, Read};



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
    pub anchors: Vec<Anchor>,
    pub categories: Vec<Category>
}

impl Page {
    /// Create a new Page object, extracting links and categories from the text
    /// of the page.
    pub fn new(title: String, page: String) -> Self {
        Page {
            title,
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
pub struct PageAnchorIterator<R: Read> {
    reader: qx::Reader<BufReader<R>>,
    buf: Vec<u8>,
    page_buf: Vec<u8>,
    title: String,
}


impl<R: Read> PageAnchorIterator<R> {
    pub fn new(xml_stream: BufReader<R>) -> Self {
        PageAnchorIterator {
            reader: qx::Reader::from_reader(xml_stream),
            buf: vec![],
            page_buf: vec![],
            title: String::new()
        }
    }

    fn extract_title(&mut self) {
        self.title = self.reader.read_text(b"title", &mut self.page_buf).unwrap();
    }
}


impl<R: Read> Iterator for PageAnchorIterator<R> {
    type Item = Page;

    fn next(&mut self) -> Option<Self::Item> {
        enum Tag {
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
                Tag::Title => self.extract_title(),
                Tag::Text => {
                    match self.reader.read_text(b"text", &mut self.page_buf) {
                        Ok(page) => return Some(Page::new(self.title.clone(), page)),
                        Err(_) => return None,
                    }
                }
                _ => ()
            }
        }
        None
    }
}
