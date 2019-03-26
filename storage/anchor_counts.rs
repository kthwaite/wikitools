use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

use qp_trie::{wrapper::BString, Trie};
use rayon::prelude::*;

use core::{
    bisect::chunk_file,
    indices::WikiDumpIndices
};
use crate::page::{
    Anchor,
    Page,
}; 

pub type AnchorCounts = Trie<BString, u32>;

fn format_anchor(anchor: Anchor) -> (String, String) {
    match anchor {
        Anchor::Direct(name) => {
            let name = name.trim();
            (name.to_lowercase(), name.to_string())
        }
        Anchor::Label { surface, page } => (surface.trim().to_lowercase(), page.trim().to_string()),
    }
}

/// Count anchors from a wikidump multistream, returning a map of surface forms.
pub fn extract_anchor_counts(indices: &WikiDumpIndices, data: &Path) -> AnchorCounts {
    let mut indices = indices.keys().collect::<Vec<_>>();
    let pbar = Mutex::new(pbr::ProgressBar::new(indices.len() as u64));
    indices.sort();

    let anchor_counts: AnchorCounts = Trie::new();
    let anchor_counts = Mutex::new(anchor_counts);

    indices
        .into_par_iter()
        .map(|index| {
            let pages = Page::index_to_pages(data, *index);
            let anchors: AnchorCounts =
                pages
                    .into_iter()
                    .map(|page| page.anchors)
                    .fold(Trie::new(), |mut acc, anchors| {
                        anchors
                            .into_iter()
                            .map(format_anchor)
                            .for_each(|(surf_form, entity)| {
                                let pair = format!("{}\t{}", surf_form, entity);
                                *acc.entry(pair.into()).or_insert(0) += 1;
                            });
                        acc
                    });
            anchors
        })
        .for_each(|trie| {
            {
                let mut acc = anchor_counts.lock().unwrap();
                for (pair, count) in trie {
                    *acc.entry(pair).or_insert(0) += count;
                }
            }
            {
                let mut prog_bar = pbar.lock().unwrap();
                prog_bar.inc();
            }
        });
    anchor_counts.into_inner().unwrap()
}

// Consume an anchor summary file and return a map of surface forms.
pub fn extract_anchor_counts_from_anchors<P: AsRef<Path>>(
    anchor_file: P,
    chunk_len: Option<u64>,
) -> io::Result<Trie<BString, u32>> {
    let anchor_counts: Trie<BString, u32> = Trie::new();
    let anchor_counts = Mutex::new(anchor_counts);
    let chunk_len = chunk_len.unwrap_or(256 * 1024 * 1024);
    let chunks = chunk_file(anchor_file, chunk_len)?;
    let pbar = Mutex::new(pbr::ProgressBar::new(chunks.len() as u64));

    chunks.into_par_iter().for_each(|(start, end)| {
        let mut file = File::open("anchors-2019-01-10").unwrap();
        file.seek(SeekFrom::Start(start)).unwrap();
        let file = file.take(end - start);
        let file = BufReader::with_capacity(128 * 1024 * 1024, file);
        let mut trie: Trie<BString, u32> = Trie::new();
        file.lines().map(|line| line.unwrap()).for_each(|line| {
            let mut line = line.rsplit('\t');
            let en = line.next().unwrap_or("").trim();
            let sf = line.next().unwrap_or("").trim().to_lowercase();
            *trie.entry(format!("{}\t{}", sf, en).into()).or_insert(0) += 1;
        });
        {
            let mut acc = anchor_counts.lock().unwrap();
            for (pair, count) in trie {
                *acc.entry(pair).or_insert(0) += count;
            }
        }
        {
            let mut prog_bar = pbar.lock().unwrap();
            prog_bar.inc();
        }
    });
    Ok(anchor_counts.into_inner().unwrap())
}

// Consume an anchor summary file and return a map of surface forms.
pub fn extract_anchor_counts_from_anchors_nested<P: AsRef<Path> + Send + Sync + Copy>(
    anchor_file: P,
    chunk_len: Option<u64>,
) -> io::Result<Trie<BString, Trie<BString, u32>>> {
    let anchor_counts: Trie<BString, Trie<BString, u32>> = Trie::new();
    let anchor_counts = Mutex::new(anchor_counts);
    let chunk_len = chunk_len.unwrap_or(256 * 1024 * 1024);
    let chunks = chunk_file(anchor_file, chunk_len)?;
    let pbar = Mutex::new(pbr::ProgressBar::new(chunks.len() as u64));

    chunks.into_par_iter().for_each(|(start, end)| {
        let mut file = File::open(anchor_file).unwrap();
        file.seek(SeekFrom::Start(start)).unwrap();
        let file = file.take(end - start);
        let file = BufReader::with_capacity(chunk_len as usize, file);
        let mut trie: Trie<BString, Trie<BString, u32>> = Trie::new();
        file.lines().map(|line| line.unwrap()).for_each(|line| {
            let mut line = line.rsplit('\t');
            let entity = line.next().unwrap_or("").trim();
            let surface_form = line.next().unwrap_or("").trim().to_lowercase();
            *trie
                .entry(surface_form.into())
                .or_insert_with(Trie::new)
                .entry(entity.into())
                .or_insert(0) += 1;
        });
        {
            let mut acc = anchor_counts.lock().unwrap();
            acc.extend(trie.into_iter());
        }
        {
            let mut prog_bar = pbar.lock().unwrap();
            prog_bar.inc();
        }
    });
    Ok(anchor_counts.into_inner().unwrap())
}

/// Write a map of anchor counts.
pub fn write_anchor_counts<W: Write>(
    anchor_counts: AnchorCounts,
    writer: &mut W,
) -> io::Result<()> {
    for (pair, count) in anchor_counts.into_iter().filter(|(_, count)| *count > 1) {
        writeln!(writer, "{}\t{}", pair.as_str(), count)?;
    }
    Ok(())
}

pub fn merge_surface_forms<P: AsRef<Path>>(anchor_file_path: P) -> io::Result<()> {
    let _anchor_counts = extract_anchor_counts_from_anchors(anchor_file_path, None)?;
    // extract_titles(titles_path)
    // extract_redirects(redirects_path)
    Ok(())
}
