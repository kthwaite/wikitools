use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};

lazy_static! {
    /// Check if a wikipedia anchor links to an external resource.
    static ref EXT_LINK : Regex = Regex::new("^[A-Za-z]+:").unwrap();
}

/// Wikipedia anchor, representing a link between pages, optionally with a
/// surface realisation.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum Anchor {
    Direct(String),
    Label { surface: String, page: String },
}

impl Anchor {
    /// Parse an anchor string, returning an Anchor.
    /// 
    /// We consider two forms:
    /// - [[abc]] is seen as "abc" in text and links to page "abc".
    /// - [[a|b]] is labelled "b" but links to page "a".
    pub fn parse(anchor: &str) -> Self {
        match anchor.find('|') {
            Some(index) => {
                let page = anchor[..index].trim();
                let page = match page.find('#') {
                    None => page.to_owned(),
                    Some(index) => page[..index].to_owned(),
                };
                let surface = anchor[index + 1..].trim().trim_matches('\'');
                if surface.is_empty() {
                    Anchor::Direct(page)
                } else {
                    Anchor::Label {
                        page,
                        surface: surface.to_owned(),
                    }
                }
            }
            None => Anchor::Direct(anchor.trim().to_owned()),
        }
    }

    /// Extract the text of an anchor, given a start index within a string
    /// slice.
    pub fn pare_anchor_match(page: &str, begin: usize) -> Option<&str> {
        let initial = &page[begin + 2..];
        if initial.starts_with("#") {
            return None;
        }
        if EXT_LINK.is_match(initial) {
            return None;
        }
        page[begin..]
            .find("]]")
            .and_then(|end| Some(&page[begin + 2..begin + end]))
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


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_direct_anchor_trims_whitespace() {
        let anchor = Anchor::parse("  page");
        assert_eq!(anchor, Anchor::Direct("page".to_owned()));
    }

    #[test]
    fn test_label_anchor_trims_whitespace() {
        let anchor = Anchor::parse("  page  | label  ");
        assert_eq!(anchor, Anchor::Label{
            surface: "label".to_owned(),
            page: "page".to_owned(),
        });
    }
}