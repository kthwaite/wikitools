use std::fs::File;
use std::io::{self, prelude::*, BufRead, BufReader, BufWriter, SeekFrom};
use std::path::Path;
use std::sync::Mutex;

use bzip2::{read::BzDecoder, Decompress, Status};

use crate::indices::WikiDumpIndices;

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

#[derive(Debug)]
pub struct LineView<'a, B: 'a> {
    buf: &'a mut B,
}

impl<'a, B: BufRead> LineView<'a, B> {
    pub fn from_buf(buf: &'a mut B) -> Self {
        LineView { buf }
    }
}

impl<'a, B: BufRead> Iterator for LineView<'a, B> {
    type Item = String;
    fn next(&mut self) -> Option<String> {
        let mut line = String::new();
        match self.buf.read_line(&mut line) {
            Ok(0) => None,
            Ok(_) => {
                if line.ends_with('\n') {
                    line.pop();
                    if line.ends_with('\r') {
                        line.pop();
                    }
                }
                Some(line)
            }
            Err(_e) => None,
        }
    }
}

pub struct BzDecoderMulti<R> {
    pub done: bool,
    pub data: Decompress,
    pub obj: R,
    pub is_eof: bool,
}

impl<R> BzDecoderMulti<R> {
    pub fn new(obj: R) -> Self {
        BzDecoderMulti {
            done: false,
            data: Decompress::new(false),
            obj,
            is_eof: false,
        }
    }
    pub fn reset(&mut self) {
        self.data = Decompress::new(false);
        self.done = false;
    }

    pub fn in_bytes(&self) -> usize {
        self.data.total_in() as usize
    }
}

impl<R: BufRead> Read for BzDecoderMulti<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.done {
            return Ok(0);
        }

        loop {
            let (read, consumed, ret);
            {
                let input = self.obj.fill_buf()?;
                self.is_eof = input.is_empty();
                let before_out = self.data.total_out();
                let before_in = self.data.total_in();
                ret = self.data.decompress(input, buf);
                read = (self.data.total_out() - before_out) as usize;
                consumed = (self.data.total_in() - before_in) as usize;
            }
            self.obj.consume(consumed);

            let ret = ret.map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            if ret == Status::StreamEnd {
                self.done = true;
                return Ok(read);
            }
            if read > 0 || self.is_eof || buf.is_empty() {
                return Ok(read);
            }
        }
    }
}

pub struct BZipMultiStream<R> {
    reader: BufReader<BzDecoderMulti<R>>,
    done: bool,
    pub bytes: usize,
}

impl<R: BufRead> BZipMultiStream<R> {
    pub fn new(source: R) -> Self {
        BZipMultiStream {
            reader: BufReader::new(BzDecoderMulti::new(source)),
            done: false,
            bytes: 0,
        }
    }

    pub fn done(&self) -> bool {
        self.done
    }

    pub fn cycle(&mut self) -> bool {
        let decoder = self.reader.get_mut();
        self.bytes += decoder.in_bytes();
        if decoder.is_eof {
            self.done = true;
            false
        } else {
            decoder.reset();
            true
        }
    }

    pub fn lines(&mut self) -> LineView<BufReader<BzDecoderMulti<R>>> {
        LineView::from_buf(&mut self.reader)
    }
}

impl BZipMultiStream<BufReader<File>> {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let source = BufReader::new(file);
        Ok(Self::new(source))
    }
}

/// Extract one file to disk.
pub fn extract_one(
    indices: &WikiDumpIndices,
    index: usize,
    data: &Path,
    out: &str,
) -> Result<(), io::Error> {
    let mut indices = indices.keys().collect::<Vec<_>>();
    indices.sort();
    let index = indices[index];
    let reader = open_seek_bzip(data, *index).unwrap();
    let out_file = File::create(out).unwrap();
    let mut out_buf = BufWriter::with_capacity(8192 * 2, out_file);
    reader.lines().map(|l| l.unwrap()).for_each(|line| {
        writeln!(&mut out_buf, "{}", line).unwrap();
    });
    Ok(())
}


/// Try to create a new BufWriter with the given buffer size wrapped in a mutex.
///
/// # Arguments
/// * `out_path` - Output path
/// * `buf_size` - Buffer size for BufWriter
pub fn mutex_bufwriter<P: AsRef<Path>>(
    out_path: P,
    buf_size: usize,
) -> io::Result<Mutex<BufWriter<File>>> {
    let writer = File::create(out_path)?;
    let writer = if buf_size == 0 {
        BufWriter::new(writer)
    } else {
        BufWriter::with_capacity(buf_size, writer)
    };
    Ok(Mutex::new(writer))
}


/// Bisect a buffer between the given start and end bounds.
pub fn bisect_buffer_with_bounds<R: BufRead + Seek>(
    buf: &mut R,
    with_start: u64,
    with_end: u64,
) -> io::Result<u64> {
    assert!(with_end > with_start);
    let bisector = (with_end - with_start) / 2;
    buf.seek(SeekFrom::Start(with_start))?;
    buf.seek(SeekFrom::Current(bisector as i64))?;
    let mut linebuf = vec![];
    let idx = buf.read_until(b'\n', &mut linebuf)?;
    Ok(bisector + idx as u64)
}

/// Recursively bisect the buffer to the target size between the given start and end.
fn bisect_buffer_recursive_impl<R: BufRead + Seek>(
    buf: &mut R,
    curr: &mut Vec<(u64, u64)>,
    target_size: u64,
    start: u64,
    end: u64,
) -> io::Result<()> {
    if end - start <= target_size {
        curr.push((start, end));
        return Ok(());
    }
    let bisector = bisect_buffer_with_bounds(buf, start, end)?;
    if ((end - bisector) <= target_size) || ((bisector - start) <= target_size) {
        curr.push((start, bisector));
        curr.push((bisector, end));
        return Ok(());
    }
    bisect_buffer_recursive_impl(buf, curr, target_size, start, start + bisector)?;
    bisect_buffer_recursive_impl(buf, curr, target_size, start + bisector, end)?;
    Ok(())
}

/// Recursively bisect a buffer until the chunk size reaches a given boundary.
pub fn bisect_buffer_recursive<R: BufRead + Seek>(
    buf: &mut R,
    target_size: u64,
) -> io::Result<Vec<(u64, u64)>> {
    let end = buf.seek(SeekFrom::End(0))?;
    let cap = (end / target_size) as usize;
    let mut curr = Vec::with_capacity(cap);
    bisect_buffer_recursive_impl(buf, &mut curr, target_size, 0, end)?;
    Ok(curr)
}

/// Split a file into chunks not smaller than a given length, returning byte indices
/// for the start and end of each chunk.
pub fn chunk_file<P: AsRef<Path>>(file: P, chunk_len: u64) -> io::Result<Vec<(u64, u64)>> {
    let file = File::open(file)?;
    let mut file = BufReader::new(file);
    bisect_buffer_recursive(&mut file, chunk_len)
}
