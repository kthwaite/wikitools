use env_logger;
use log::info;
use qp_trie::{
    wrapper::{BStr, BString},
    Trie,
};
use storage::qpt::read_from_qpt;

fn main() -> Result<(), Box<std::error::Error>> {
    env_logger::init();
    info!("Loading anchor counts...");
    let anchor_counts: Trie<BString, u32> = read_from_qpt("anchor-counts-flat.qpt", None)?;

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
