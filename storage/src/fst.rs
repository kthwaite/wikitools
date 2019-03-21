use fst::{self, Map, MapBuilder, IntoStreamer};
use fst_regex::Regex;
use std::io::{BufWriter};
use std::fs::File;
use log::info;
use qp_trie::{Trie, wrapper::BString};
use std::path::Path;
use std::time::Instant;

use crate::surface_form::SurfaceForm;

/// Build and serialise a FST from a Trie of flat anchors.
fn build_fst_from_anchors(anchor_counts: Trie<BString, u32>, output_path: &Path) -> Result<(), Box<std::error::Error>> {
    info!("Stripping anchors...");
    let now = Instant::now();

    let mut anchors = anchor_counts.into_iter()
                .map(|(key, value)| (key.into(), value as u64))
                .collect::<Vec<(String, u64)>>();

    info!("Done in {} seconds", now.elapsed().as_secs());


    info!("Sorting anchors...");
    let now = Instant::now();

        anchors.sort_by(|(k1, _), (k2, _)| k1.partial_cmp(k2).unwrap());

    info!("Done in {} seconds", now.elapsed().as_secs());

    let file = File::create(output_path)?;
    let buf = BufWriter::with_capacity(256 * 1024 * 1024, file);
    let mut bld = MapBuilder::new(buf)?;

    info!("Building FST...");
    let now = Instant::now();

    bld.extend_iter(anchors.into_iter())?;
    bld.finish().unwrap();

    info!("Done in {} seconds", now.elapsed().as_secs());
    Ok(())
}


pub struct WikiAnchors {
    anchors: Map
}

impl WikiAnchors {
    pub fn new(path: &str) -> fst::Result<Self> {
        let anchors = unsafe { Map::from_path(path) }?;
        Ok(WikiAnchors {
            anchors
        })
    }

    /// Fetch a map of entity, count for the surface form, if any
    pub fn entities_for_query(
        &self,
        query: &str,
    ) -> Result<SurfaceForm, Box<std::error::Error>> {
        let re = Regex::new(&format!("{}\t.*", query))?;
        // TODO: semantics of returning 'no match'?
        let stream = self.anchors.search(&re).into_stream().into_str_vec()?;
        Ok(SurfaceForm::from_paired_matches(query, stream))
    }
}
