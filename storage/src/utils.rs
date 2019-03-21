use std::fs::File;
use std::io::{self, prelude::*, BufReader, SeekFrom};
use std::path::Path;

use bzip2::read::BzDecoder;

type BZipReader = BufReader<BzDecoder<BufReader<File>>>;

/// Create a bzip2 BufReader from a File handle.
pub fn to_decode_buffer(file: File) -> BZipReader {
    let buf = BufReader::with_capacity(8192 * 4, file);
    let dec = BzDecoder::new(buf);
    BufReader::with_capacity(8192 * 16, dec)
}

/// Open a bzip2 file.
pub fn open_bzip<P: AsRef<Path>>(path: P) -> io::Result<BZipReader> {
    let file = File::open(path)?;
    Ok(to_decode_buffer(file))
}

/// Open a bzip2 multistream and seek to a zip file at a given index.
pub fn open_seek_bzip<P: AsRef<Path>>(path: P, index: usize) -> io::Result<BZipReader> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(index as u64))?;
    Ok(to_decode_buffer(file))
}
