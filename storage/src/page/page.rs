use serde::{Deserialize, Serialize};

use crate::page::anchor::Anchor;
use crate::page::category::Category;

/// Collection of Anchors and Categories for a Wikipedia page.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Page {
    pub title: String,
    pub id: String,
    pub anchors: Vec<Anchor>,
    pub categories: Vec<Category>,
}

impl Page {
    /// Create a new Page object, extracting links and categories from the text
    /// of the page.
    pub fn new(title: String, id: String, page: &str) -> Self {
        Page {
            title,
            id,
            anchors: Page::extract_anchors(&page),
            categories: Page::extract_categories(&page),
        }
    }

    /// Return the title with all text after the first '(' or ',' stripped.
    pub fn title_stripped(&self) -> String {
        self.title.clone()
    }

    /// Extract category links from the text of a Wikipedia page, returning a
    /// Vec of Category objects.
    pub fn extract_categories(page: &str) -> Vec<Category> {
        let page = match page.rfind("==References==") {
            Some(index) => &page[index..],
            None => page,
        };
        page.match_indices("[[")
            .filter_map(|(begin, _)| {
                let initial = &page[begin + 2..];
                if initial.starts_with("Category:") {
                    return initial
                        .find("]]")
                        .and_then(|end| match initial[..end].find('|') {
                            Some(actual_end) => Some(&initial[9..actual_end]),
                            None => Some(&initial[9..end]),
                        });
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
            None => page,
        };
        page.match_indices("[[")
            .filter_map(|(begin, _)| Anchor::pare_anchor_match(page, begin))
            .map(Anchor::parse)
            .collect::<Vec<_>>()
    }
}
