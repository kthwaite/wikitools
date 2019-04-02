use super::anchor::Anchor;
use super::Page;
use core::indices::WikiDumpIndices;
use rayon::prelude::*;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Mutex;

pub trait PageWriter {
    /// Write Page data.
    fn write<W: Write>(page: Page, writer: &mut W) -> io::Result<()>;
}

/// Write page categories.
pub struct CategoryWriterTSV;
pub struct CategoryWriterJSONL;

impl PageWriter for CategoryWriterTSV {
    fn write<W: Write>(page: Page, writer: &mut W) -> io::Result<()> {
        let categories = page
            .categories
            .into_iter()
            .map(|cat| cat.0)
            .map(|cat| format!(r#""{}""#, cat.replace("\"", "\\\"")))
            .collect::<Vec<_>>()
            .join(",");
        writeln!(writer, "{}\t{}\t{}", page.id, page.title, categories)
    }
}

impl PageWriter for CategoryWriterJSONL {
    fn write<W: Write>(page: Page, writer: &mut W) -> io::Result<()> {
        let categories = page
            .categories
            .into_iter()
            .map(|cat| cat.0)
            .map(|anchor| format!(r#""{}""#, anchor.replace("\"", "\\\"")))
            .collect::<Vec<_>>()
            .join(", ");
        writeln!(
            writer,
            "{{ \"title\": \"{}\", \"id\": {}, \"categories\": [{}] }},",
            page.title, page.id, categories
        )
    }
}

/// Write Page Anchors.
pub struct AnchorWriterTSV;
pub struct AnchorWriterJSONL;

impl PageWriter for AnchorWriterTSV {
    fn write<W: Write>(item: Page, writer: &mut W) -> io::Result<()> {
        for anchor in item.anchors {
            match anchor {
                Anchor::Direct(name) => {
                    writeln!(writer, "{}\t{}\t{}\t{}", item.id, item.title, name, name)?;
                }
                Anchor::Label { surface, page } => {
                    writeln!(writer, "{}\t{}\t{}\t{}", item.id, item.title, surface, page)?;
                }
            }
        }
        Ok(())
    }
}

impl PageWriter for AnchorWriterJSONL {
    fn write<W: Write>(page: Page, writer: &mut W) -> io::Result<()> {
        let anchors = page
            .anchors
            .into_iter()
            .map(|anchor| match anchor {
                Anchor::Direct(name) => name,
                Anchor::Label { page, .. } => page,
            })
            .map(|anchor| format!(r#""{}""#, anchor.replace("\"", "\\\"")))
            .collect::<Vec<_>>()
            .join(", ");
        writeln!(
            writer,
            "{{ \"title\": \"{}\", \"id\": {}, \"anchors\": [{}] }},",
            page.title, page.id, anchors
        )
    }
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
        let pages = Page::index_to_pages(data, *index);
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
