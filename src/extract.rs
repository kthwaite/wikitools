use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;

use fnv::FnvHashMap;
use pbr;
use rayon::prelude::*;
use tantivy::{schema::*, IndexWriter};

use crate::indices::WikiDumpIndices;
use crate::page::{Anchor, Page, PageIterator, PageWriter};
use crate::utils::open_seek_bzip;

/// Extract a vector of Pages from the zipped store at a given index in a
/// Wikipedia dump.
fn index_to_pages(data: &Path, index: &usize) -> Vec<Page> {
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



pub fn extract_anchor_counts(
    indices: &WikiDumpIndices,
    data: &Path,
) -> HashMap<String, HashMap<String, usize>> {
    let mut indices = indices.keys().collect::<Vec<_>>();
    let pbar = Mutex::new(pbr::ProgressBar::new(indices.len() as u64));
    indices.sort();

    indices.into_par_iter().map(|index| {
        let pages = index_to_pages(data, index);
        let anchors : HashMap<String, HashMap<String, usize>> = pages
            .into_iter()
            .map(|page| page.anchors)
            .fold(HashMap::default(), |mut acc, anchors| {
                for anchor in anchors {
                    let (surf_form, entity) : (String, String) = match anchor {
                        Anchor::Direct(name) => {
                            let name = name.trim();
                            (name.to_lowercase(), name.to_string())
                        },
                        Anchor::Label { surface, page } => (surface.trim().to_lowercase(), page.trim().to_string()),
                    };

                    acc.entry(surf_form)
                        .or_insert_with(HashMap::default)
                        .entry(entity.to_owned())
                        .and_modify(|v| *v += 1)
                        .or_insert(1);
                }
                acc
            });
        anchors
    })
    .reduce(HashMap::default, |mut acc, map| {
        for (key, value) in map.into_iter() {
            let val = acc.entry(key)
                        .or_insert_with(HashMap::default);
            for (sk, sv) in value.into_iter() {
                val.entry(sk)
                    .and_modify(|v| *v += sv)
                    .or_insert(sv);
            }
        }
        {
            let mut prog_bar = pbar.lock().unwrap();
            prog_bar.inc();
        }
        acc
    })
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

/// Write page-page anchor counts.
pub fn write_anchor_counts<R: BufRead>(anchors: R, out_path: &Path) -> io::Result<()> {
    let mut counter: FnvHashMap<String, usize> = Default::default();

    for (index, line) in anchors.lines().map(|line| line.unwrap()).enumerate() {
        let quad = line.split('\t').collect::<Vec<_>>();

        if quad.len() < 5 {
            continue;
        }

        let sf = quad[2];
        let en = quad[3];

        // Nested dicts will use ~5-10x RAM.
        let sfen = format!("{}\t{}", sf, en);

        counter.entry(sfen).and_modify(|v| *v += 1).or_insert(1);

        if index % 100_000 == 0 {
            println!("Processed {} lines", index);
        }
    }

    println!("Done, writing final dump to {:?}", out_path);
    let anchor_file = File::create(out_path)?;
    let mut writer = BufWriter::with_capacity(8192 * 1024, anchor_file);

    for (sf_en, count) in counter {
        writeln!(&mut writer, "{}\t{}", sf_en, count)?;
    }

    Ok(())
}
