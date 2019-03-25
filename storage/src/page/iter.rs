use super::page::Page;
use quick_xml::{self as qx, events::Event};
use std::io::{BufReader, Read};

/// Iterator yielding Page objects for an XML file.
pub struct PageIterator<R: Read> {
    reader: qx::Reader<BufReader<R>>,
    buf: Vec<u8>,
    page_buf: Vec<u8>,
    title: String,
    id: String,
}

impl<R: Read> PageIterator<R> {
    /// Create a new iterator from an XML source.
    pub fn new(xml_stream: BufReader<R>) -> Self {
        PageIterator {
            reader: qx::Reader::from_reader(xml_stream),
            buf: vec![],
            page_buf: vec![],
            title: String::new(),
            id: String::new(),
        }
    }

    /// Capture and set the title of the current page.
    fn extract_title(&mut self) {
        if let Ok(title) = self.reader.read_text(b"title", &mut self.page_buf) {
            self.title = title;
        }
    }

    /// Capture and set the id of the current page.
    fn extract_id(&mut self) {
        if let Ok(id) = self.reader.read_text(b"id", &mut self.page_buf) {
            self.id = id;
        }
    }

    // TODO: use regex here, with custom filters.
    fn is_filtered_title(&self) -> bool {
        // Skip over files.
        self.title.starts_with("File:")
        // Skip over templates.
        || self.title.starts_with("Template:")
        // Skip over Wikipedia internal pages.
        || self.title.starts_with("Wikipedia:")
        // Skip over User talk.
        || self.title.starts_with("User talk:")
        // Skip over File talk.
        || self.title.starts_with("File talk:")
    }
}

impl<R: Read> Iterator for PageIterator<R> {
    type Item = Page;

    fn next(&mut self) -> Option<Self::Item> {
        enum Tag {
            Id,
            Text,
            Title,
            Redirect,
            None,
        }
        loop {
            let action = {
                match self.reader.read_event(&mut self.buf) {
                    Ok(Event::Start(ref tag)) => match tag.name() {
                        b"text" => Tag::Text,
                        b"id" => Tag::Id,
                        b"title" => Tag::Title,
                        _ => Tag::None,
                    },
                    Ok(Event::Empty(ref tag)) => match tag.name() {
                        b"redirect" => Tag::Redirect,
                        _ => Tag::None,
                    },
                    Ok(Event::Eof) => break,
                    Ok(_) => Tag::None,
                    Err(_) => break,
                }
            };
            match action {
                Tag::Id => self.extract_id(),
                Tag::Title => self.extract_title(),
                Tag::Redirect => {
                    // Skip over redirects; these are handled separately.
                    self.reader
                        .read_to_end(b"page", &mut self.page_buf)
                        .unwrap();
                }
                Tag::Text => {
                    // Don't skip Portal pages for now.
                    if self.is_filtered_title() {
                        continue;
                    }
                    match self.reader.read_text(b"text", &mut self.page_buf) {
                        Ok(page) => {
                            return Some(Page::new(self.title.clone(), self.id.clone(), &page));
                        }
                        Err(_) => return None,
                    }
                }
                _ => (),
            }
        }
        None
    }
}

/// Iterator yielding String for each page in an XML file.
pub struct RawPageIterator<R: Read>(pub PageIterator<R>);

impl<R: Read> Iterator for RawPageIterator<R> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.0.reader.read_event(&mut self.0.buf) {
                Ok(Event::Start(ref tag)) => match tag.name() {
                    b"text" => {
                        if self.0.is_filtered_title() {
                            continue;
                        }
                        match self.0.reader.read_text(b"text", &mut self.0.page_buf) {
                            Ok(page) => {
                                return Some(page);
                            }
                            Err(_) => return None,
                        }
                    }
                    _ => (),
                },
                Ok(Event::Empty(ref tag)) => match tag.name() {
                    b"redirect" => {
                        self.0
                            .reader
                            .read_to_end(b"page", &mut self.0.page_buf)
                            .unwrap();
                    }
                    _ => (),
                },
                Ok(Event::Eof) => break,
                Ok(_) => (),
                Err(_) => break,
            }
        }
        None
    }
}

/// Iterator yielding String for each page in an XML file.
pub struct TantivyPageIterator<R: Read>(pub PageIterator<R>);

impl<R: Read> Iterator for TantivyPageIterator<R> {
    type Item = (String, String, String);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.0.reader.read_event(&mut self.0.buf) {
                Ok(Event::Start(ref tag)) => match tag.name() {
                    b"id" => self.0.extract_id(),
                    b"title" => self.0.extract_title(),
                    b"text" => {
                        if self.0.is_filtered_title() {
                            continue;
                        }
                        match self.0.reader.read_text(b"text", &mut self.0.page_buf) {
                            Ok(page) => {
                                return Some((self.0.id.clone(), self.0.title.clone(), page));
                            }
                            Err(_) => return None,
                        }
                    }
                    _ => (),
                },
                Ok(Event::Empty(ref tag)) => match tag.name() {
                    b"redirect" => {
                        self.0
                            .reader
                            .read_to_end(b"page", &mut self.0.page_buf)
                            .unwrap();
                    }
                    _ => (),
                },
                Ok(Event::Eof) => break,
                Ok(_) => (),
                Err(_) => break,
            }
        }
        None
    }
}
