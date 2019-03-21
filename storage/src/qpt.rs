use bincode;
use qp_trie::{Trie, wrapper::BString};
use std::io::{BufReader, BufWriter};
use std::time::Instant;
use std::path::Path;
use log::info;
use std::fs::File;

/// Serialize a Trie into a .qpt binary file.
pub fn write_to_qpt<P>(
    anchor_counts: &Trie<BString, u32>,
    path: P,
    buf_size: Option<usize>,
) -> bincode::Result<()>
where
    P: AsRef<Path>
{
    let file = File::create(path)?;
    let buf_size = buf_size.unwrap_or(256 * 1024 * 1024);
    let file = BufWriter::with_capacity(buf_size, file);
    bincode::serialize_into(file, &anchor_counts)
}

/// Deserialise a Trie from a .qpt binary file.
pub fn read_from_qpt<P>(
    anchor_counts_flat_path: P,
    buf_size: Option<usize>,
)  -> bincode::Result<Trie<BString, u32>>
where
    P: AsRef<Path>
{
    info!("Loading anchor counts...");

    let now = Instant::now();
    let file = File::open(anchor_counts_flat_path)?;
    let buf_size = buf_size.unwrap_or(256 * 1024 * 1024);
    let reader = BufReader::with_capacity(buf_size, file);
    let anchor_counts: Trie<BString, u32> = bincode::deserialize_from(reader)?;

    info!("Done in {} seconds", now.elapsed().as_secs());
    Ok(anchor_counts)
}