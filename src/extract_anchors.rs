use quick_xml::{
    self as qx,
    events::{Event, BytesStart}
};
use std::io::{BufReader, Read};


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
                let surface = anchor[index+1..].to_owned();
                Anchor::Label{page, surface}
            },
            None => Anchor::Direct(anchor.to_owned())
        }
    }

    /// Check if an anchor string points to a file.
    pub fn is_file(anchor: &str) -> bool {
        anchor.starts_with("File:") || anchor.starts_with("Image:")
    }
}

#[derive(Debug, Default, Clone)]
pub struct Page {
    title: String,
    anchors: Vec<Anchor>
}

impl Page {
    fn new(title: String, page: String) -> Self {
        Page {
            title,
            anchors: Page::extract_anchors(&page)
        }
    }

    fn extract_anchors(page: &str) -> Vec<Anchor> {
        page.match_indices("[[")
            .filter_map(|(begin, _)| {
                let initial = &page[begin + 2..];
                if Anchor::is_file(initial) {
                    return None;
                }
                page[begin..].find("]]").and_then(|end| {
                    Some(&page[begin + 2..begin + end])
                })
            })
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
