use fst::{self, IntoStreamer, Map, MapBuilder};
use fst_regex::Regex;
use log::info;
use qp_trie::{wrapper::BString, Trie};
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::time::Instant;

use crate::surface_form::{SurfaceForm, SurfaceFormStoreError, SurfaceFormStoreRead};

impl std::convert::From<fst::Error> for SurfaceFormStoreError {
    fn from(error: fst::Error) -> Self {
        SurfaceFormStoreError::Generic(error.to_string())
    }
}

impl std::convert::From<fst_regex::Error> for SurfaceFormStoreError {
    fn from(error: fst_regex::Error) -> Self {
        SurfaceFormStoreError::Generic(error.to_string())
    }
}

/// Build and serialise a FST from a Trie of flat anchors.
fn build_fst_from_anchors(
    anchor_counts: Trie<BString, u32>,
    output_path: &Path,
) -> Result<(), Box<std::error::Error>> {
    info!("Stripping anchors...");
    let now = Instant::now();

    let mut anchors = anchor_counts
        .into_iter()
        .map(|(key, value)| (key.into(), u64::from(value)))
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
    anchors: Map,
}

impl WikiAnchors {
    pub fn new(path: &str) -> fst::Result<Self> {
        let anchors = unsafe { Map::from_path(path) }?;
        Ok(WikiAnchors { anchors })
    }
}

impl SurfaceFormStoreRead for WikiAnchors {
    /// Fetch a map of entity, count for the surface form, if any.
    fn get(&self, surface_form: &str) -> Result<Option<SurfaceForm>, SurfaceFormStoreError> {
        let re = Regex::new(&format!("{}\t.*", surface_form))?;
        let stream = self.anchors.search(&re).into_stream().into_str_vec()?;
        // let stream = self.anchors.range()
        //     .ge(&format!("{}\t", query))
        //     .lt(&format!("{}a\t", query))
        //     .into_stream()
        //     .into_str_vec()?;
        Ok(Some(SurfaceForm::from_paired_matches(surface_form, stream)))
    }
}
