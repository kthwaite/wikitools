use crate::page::{Anchor, Page};
use std::io::{self, Write};

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
