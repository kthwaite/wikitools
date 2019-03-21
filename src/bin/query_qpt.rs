use log::info;
use qp_trie::{
    wrapper::{BStr, BString},
    Trie,
};
use std::fs::File;
use std::time::Instant;
use std::io::{BufReader};
use bincode;


fn main() -> Result<(), Box<std::error::Error>> {
    info!("Loading anchor counts...");
    let start = Instant::now();
    let file = File::open("anchor-counts-flat.qpt")?;
    let reader = BufReader::with_capacity(256 * 1024 * 1024, file);
    let anchor_counts: Trie<BString, u32> = bincode::deserialize_from(reader)?;
    info!("Done in {} seconds", start.elapsed().as_secs());

    info!("Searching for surface form: `EU`...");
    let prefix = AsRef::<BStr>::as_ref("eu\t");
    let mut ret = anchor_counts
        .iter_prefix(prefix)
        .map(|(key, value)| {
            let key = key.as_str();
            (key[key.find('\t').unwrap_or(0) + 1..].to_string(), value)
        })
        .collect::<Vec<_>>();
    ret.sort_by(|(_, v1), (_, v2)| v1.partial_cmp(v2).unwrap());
    for (key, val) in ret {
        println!("{} . {}", val, key);
    }
    Ok(())
}
