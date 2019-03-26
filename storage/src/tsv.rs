use core::indices::WikiDumpIndices;
use core::multistream::mutex_bufwriter;
use super::page::writer::extract_with_writer;
use super::redirect::write_redirects;


/// Dump a list of redirects to file as tab-separated pairs.
fn dump_redirects_tsv(
    page_indices: &WikiDumpIndices,
    data_dump: &Path,
    out_path: &Path,
    buf_size: usize,
) -> io::Result<()> {
    let writer = mutex_bufwriter(out_path, buf_size)?;
    write_redirects(&page_indices, &data_dump, &writer);
    Ok(())
}


/// Write anchors from a Wikipedia dump to text file.
///
/// # Arguments
/// * `indices` - Parsed Wikipedia page indices for the corresponding dump
/// * `dump` - Path to Wikipedia dump
/// * `out_path` - Output path
/// * `buf_size` - Buffer size for writer
pub fn write_anchors_tsv(
    indices: &WikiDumpIndices,
    dump: &Path,
    out_path: &Path,
    buf_size: usize,
) -> io::Result<()> {
    let writer = mutex_bufwriter(out_path, buf_size)?;

    extract_with_writer(AnchorWriterTSV, &indices, &dump, &writer);
    Ok(())
}