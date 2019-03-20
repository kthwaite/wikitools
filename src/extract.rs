use std::io;
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

use pbr;
use qp_trie::{wrapper::BString, Trie};
use rayon::prelude::*;
use serde_json;

use crate::indices::WikiDumpIndices;
use crate::page::{
    writer::{AnchorWriterJSONL, AnchorWriterTSV},
    Anchor, Page, PageIterator, PageWriter, RawPageIterator,
};
use crate::redirect::write_redirects;
use crate::utils::{mutex_bufwriter, open_seek_bzip};

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


pub struct TrieBuilderFlat;
pub struct TrieBuilderNested;

pub trait AnchorTrieBuilder<V> {
    fn fold(into: &mut Trie<BString, V>, from: Trie<BString, V>);
    fn extract(path: &Path, index: usize) -> Trie<BString, V>;
}

impl AnchorTrieBuilder<u32> for TrieBuilderFlat {
    fn fold(into: &mut Trie<BString, u32>, from: Trie<BString, u32>) {
        for (key, value) in from {
            *into.entry(key).or_insert(0) += value;
        }
    }

    /// Extract anchor counts for one file and return a flat Trie.
    ///
    /// Returned Trie maps surface forms to a Trie mapping page names to counts.
    ///
    /// # Arguments
    /// * `path` - Path to bzip2 file.
    /// * `index` - Offset within bzip2 file at which to begin reading pages.
    fn extract(path: &Path, index: usize) -> Trie<BString, u32> {
        let store = open_seek_bzip(path, index).unwrap();
        let mut chunk_counts: Trie<BString, u32> = Trie::new();

        RawPageIterator(PageIterator::new(store)).for_each(|page| {
            page.match_indices("[[")
                .filter_map(|(begin, _)| Anchor::pare_anchor_match(&page, begin))
                .filter(|anchor| {
                    !anchor.starts_with(":")
                        && !anchor.starts_with("<")
                        && !anchor.contains("User talk:")
                        && !anchor.contains("File talk:")
                })
                .map(Anchor::parse)
                .map(|anchor| match anchor {
                    Anchor::Direct(name) => (name.to_lowercase(), name),
                    Anchor::Label { surface, page } => (surface.to_lowercase(), page),
                })
                .for_each(|(surf, page)| {
                    *chunk_counts
                        .entry(format!("{}\t{}", surf.trim(), page.trim()).into())
                        .or_insert(0) += 1
                });
        });
        chunk_counts
    }
}

impl AnchorTrieBuilder<Trie<BString, u32>> for TrieBuilderNested {
    fn fold(into: &mut Trie<BString, Trie<BString, u32>>, from: Trie<BString, Trie<BString, u32>>) {
        for (key, inner) in from {
            let mut outer = into.entry(key).or_insert_with(Trie::new);
            for (ikey, value) in inner {
                *outer.entry(ikey).or_insert(0) += value;
            }
        }
    }

    /// Extract anchor counts for one file and return a nested Trie.
    ///
    /// Returned Trie maps surface forms to a Trie mapping page names to counts.
    ///
    /// # Arguments
    /// * `path` - Path to bzip2 file.
    /// * `index` - Offset within bzip2 file at which to begin reading pages.
    fn extract(path: &Path, index: usize) -> Trie<BString, Trie<BString, u32>> {
        let store = open_seek_bzip(path, index).unwrap();
        let mut chunk_counts: Trie<BString, Trie<BString, u32>> = Trie::new();

        RawPageIterator(PageIterator::new(store)).for_each(|page| {
            page.match_indices("[[")
                .filter_map(|(begin, _)| Anchor::pare_anchor_match(&page, begin))
                .map(Anchor::parse)
                .map(|anchor| match anchor {
                    Anchor::Direct(name) => (name.to_lowercase(), name),
                    Anchor::Label { surface, page } => (surface.to_lowercase(), page),
                })
                .for_each(|(surf, page)| {
                    *chunk_counts
                        .entry(surf.trim().into())
                        .or_insert_with(Trie::new)
                        .entry(page.trim().into())
                        .or_insert(0) += 1
                });
        });
        chunk_counts
    }
}

/// Extract anchor counts for a set of indices in a dump, returning a Trie.
///
/// This function allows the user to specify a method F taking the data file
/// path and an offset and producing a trie, the value of which is unspecified.
/// Thus, this function may be used to produce either a nested Trie for
/// serialization to JSON, or a 'flat' Trie for use in TSV or FST serialization.
///
/// # Arguments
/// * `indices` - Map of bzip2 multistream indices to page indices.
/// * `path` - Path to a wikipedia bzip2 multistream.
/// * `method` - Method for transforming bzip2 chunks to Tries.
pub fn extract_anchor_counts_to_trie<Builder, V>(
    _builder: Builder,
    indices: &WikiDumpIndices,
    data: &Path
) -> Trie<BString, V>
where
    V: Send + Sync,
    Builder: AnchorTrieBuilder<V>
{
    let mut indices = indices.keys().collect::<Vec<_>>();
    let pbar = Mutex::new(pbr::ProgressBar::new(indices.len() as u64));
    let anchor_counts = Mutex::new(Trie::new());
    indices.sort();

    indices.into_par_iter().for_each(|index| {
        let chunk_counts = Builder::extract(data, *index);
        {
            let mut anchor_counts = anchor_counts.lock().unwrap();
            Builder::fold(&mut anchor_counts, chunk_counts);
        }
        {
            let mut prog_bar = pbar.lock().unwrap();
            prog_bar.inc();
        }
    });
    anchor_counts.into_inner().unwrap()
}
}
