use std::fs::File;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;

use indices::WikiDumpIndices;
use fnv::FnvHashMap;
use pbr;
use rayon::prelude::*;
use tantivy::{ IndexWriter, schema::* };

use page::{Anchor, PageIterator};
use utils::open_seek_bzip;


/// Extract anchors from a Wikipedia dump, writing them to JSON.
pub fn extract_pages_json<W: Write + Send + Sync>(indices: &WikiDumpIndices, data: &Path, writer: &Mutex<W>) {
    let mut indices = indices.keys().collect::<Vec<_>>();
    let pbar = Mutex::new(pbr::ProgressBar::new(indices.len() as u64));
    indices.sort();

    use serde_json;
    indices.into_par_iter()
           .for_each(|index| {
                let store = open_seek_bzip(&data, *index).unwrap();
                let pages = PageIterator::new(store).collect::<Vec<_>>();
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
                                    writeln!(&mut w, "{}\t{}\t{}\t{}", item.id, item.title, name, name).unwrap();

                                },
                                Anchor::Label{ surface, page } => {
                                    writeln!(&mut w, "{}\t{}\t{}\t{}", item.id, item.title, surface, page).unwrap();
                                }
                            }
                        }
                    });
                }
                {
                    let mut prog_bar = pbar.lock().unwrap();
                    prog_bar.inc();
                }
            });
}

pub fn extract_anchors_jsonl<W: Write + Send + Sync>(indices: &WikiDumpIndices, data: &Path, writer: &Mutex<W>) {
    let mut indices = indices.keys().collect::<Vec<_>>();
    let pbar = Mutex::new(pbr::ProgressBar::new(indices.len() as u64));
    indices.sort();
    indices.into_par_iter()
           .for_each(|index| {
                let store = open_seek_bzip(&data, *index).unwrap();
                let pages = PageIterator::new(store).collect::<Vec<_>>();
                {
                    let mut w = writer.lock().unwrap();
                    pages.into_iter().for_each(|page| {
                        let anchors = page.anchors.into_iter()
                            .map(|anchor| {
                                match anchor {
                                    Anchor::Direct(name) => name,
                                    Anchor::Label{page, .. } => page
                                }
                            })
                            .map(|anchor| {
                                format!(r#""{}""#, anchor.replace("\"", "\\\""))
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        writeln!(w, "{{ \"title\": \"{}\", \"id\": {}, \"anchors\": [{}] }},", page.title, page.id, anchors);
                    });
                }
                {
                    let mut prog_bar = pbar.lock().unwrap();
                    prog_bar.inc();
                }
            });
}


pub fn index_anchors(indices: &WikiDumpIndices, data: &Path, indexer: &Mutex<IndexWriter>, schema: &Schema) {
    let mut indices = indices.keys().collect::<Vec<_>>();
    let pbar = Mutex::new(pbr::ProgressBar::new(indices.len() as u64));
    indices.sort();

    let (id, title, links) = (
        schema.get_field("id").unwrap(),
        schema.get_field("title").unwrap(),
        schema.get_field("links").unwrap()
    );

    indices.into_par_iter()
           .map(|index| {
                let store = open_seek_bzip(&data, *index).unwrap();
                let pages = PageIterator::new(store).collect::<Vec<_>>();
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
                                },
                                Anchor::Label{ page, .. } => {
                                    doc.add_text(links, &page);
                                }
                            }
                        }
                        doc
                    }).collect::<Vec<_>>()
           }).for_each(|docs| {
               {
                   let mut index_writer = indexer.lock().expect("Failed to unlock indexer");
                   docs.into_iter().for_each(|doc| { index_writer.add_document(doc); });
               }
               {
                   let mut prog_bar = pbar.lock().unwrap();
                   prog_bar.inc();
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


pub fn write_anchor_counts<R: BufRead>(anchors: R, out_path: &Path) -> io::Result<()> {
    let mut counter : FnvHashMap<String, usize> = Default::default();

    for (index, line) in anchors.lines().map(|line| line.unwrap()).enumerate() {
        let quad = line.split('\t').collect::<Vec<_>>();

        if quad.len() < 5 {
            continue;
        }

        let sf = quad[2];
        let en = quad[3];

        // Nested dicts will use ~5-10x RAM.
        let sfen = format!("{}\t{}", sf, en);

        counter.entry(sfen)
               .and_modify(|v| *v += 1)
               .or_insert(1);

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
