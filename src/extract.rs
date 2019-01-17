use std::io::{Write};
use std::path::Path;
use std::sync::Mutex;

use pbr;
use rayon::prelude::*;
use tantivy::{schema::*, IndexWriter};

use crate::indices::WikiDumpIndices;
use crate::page::{Anchor, Page, PageIterator, PageWriter};
use crate::utils::{open_seek_bzip};

/// Extract a vector of Pages from the zipped store at a given index in a
/// Wikipedia dump.
pub fn index_to_pages<P: AsRef<Path>>(data: P, index: &usize) -> Vec<Page> {
    let store = open_seek_bzip(&data, *index).unwrap();
    PageIterator::new(store).collect::<Vec<_>>()
}

/// Extract anchors from a Wikipedia dump, writing them to JSON.
pub fn extract_pages_json<W: Write + Send + Sync>(
    indices: &WikiDumpIndices,
    data: &Path,
    writer: &Mutex<W>,
) {
    let mut indices = indices.keys().collect::<Vec<_>>();
    let pbar = Mutex::new(pbr::ProgressBar::new(indices.len() as u64));
    indices.sort();

    use serde_json;
    indices.into_par_iter().for_each(|index| {
        let pages = index_to_pages(data, index);
        {
            let mut w = writer.lock().unwrap();
            pages.into_iter().for_each(|page| {
                writeln!(w, "{}", serde_json::to_string/*_pretty*/(&page).unwrap()).unwrap();
            });
        }
        {
            let mut prog_bar = pbar.lock().unwrap();
            prog_bar.inc();
        }
    });
}

/// Extract page data and write using the specified PageWriter.
pub fn extract_with_writer<P, W>(
    _page_writer: P,
    indices: &WikiDumpIndices,
    data: &Path,
    writer: &Mutex<W>,
) where
    P: PageWriter,
    W: Write + Send + Sync,
{
    let mut indices = indices.keys().collect::<Vec<_>>();
    let pbar = Mutex::new(pbr::ProgressBar::new(indices.len() as u64));
    indices.sort();

    indices.into_par_iter().for_each(|index| {
        let pages = index_to_pages(data, index);
        {
            let w = &mut *writer.lock().unwrap();
            pages.into_iter().for_each(|page| {
                P::write(page, w).unwrap();
            });
        }
        {
            let mut prog_bar = pbar.lock().unwrap();
            prog_bar.inc();
        }
    });
}


/// Use tantivy to index anchors for each page.
pub fn index_anchors(
    indices: &WikiDumpIndices,
    data: &Path,
    indexer: &Mutex<IndexWriter>,
    schema: &Schema,
) {
    let mut indices = indices.keys().collect::<Vec<_>>();
    let pbar = Mutex::new(pbr::ProgressBar::new(indices.len() as u64));
    indices.sort();

    let (id, title, links) = (
        schema.get_field("id").unwrap(),
        schema.get_field("title").unwrap(),
        schema.get_field("links").unwrap(),
    );

    indices
        .into_par_iter()
        .map(|index| {
            let pages = index_to_pages(data, index);
            pages
                .into_iter()
                .map(|item| {
                    let mut doc = Document::default();
                    doc.add_u64(id, item.id.parse::<u64>().unwrap());
                    doc.add_text(title, &item.title);
                    for anchor in item.anchors {
                        match anchor {
                            Anchor::Direct(name) => {
                                doc.add_text(links, &name);
                            }
                            Anchor::Label { page, .. } => {
                                doc.add_text(links, &page);
                            }
                        }
                    }
                    doc
                })
                .collect::<Vec<_>>()
        })
        .for_each(|docs| {
            {
                let mut index_writer = indexer.lock().expect("Failed to unlock indexer");
                docs.into_iter().for_each(|doc| {
                    index_writer.add_document(doc);
                });
            }
            {
                let mut prog_bar = pbar.lock().unwrap();
                prog_bar.inc();
            }
        });
}
