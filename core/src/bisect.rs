use std::fs::File;
use std::io::{self, prelude::*, BufRead, BufReader, SeekFrom};
use std::path::Path;

use log::{info, trace};


/// Bisect a buffer between the given start and end bounds.
pub fn bisect_buffer_with_bounds<R: BufRead + Seek>(
    buf: &mut R,
    with_start: u64,
    with_end: u64,
) -> io::Result<u64> {
    assert!(
        with_end > with_start,
        "with_end <= with_start in bisect_buffer_with_bounds"
    );
    let bisector = (with_end - with_start) / 2;
    buf.seek(SeekFrom::Start(with_start))?;
    buf.seek(SeekFrom::Current(bisector as i64))?;
    let mut bx = [0; 1];
    buf.read_exact(&mut bx)?;
    let idx = match bx[0] {
        b'\n' => 0,
        _ => {
            let mut linebuf = vec![];
            buf.read_until(b'\n', &mut linebuf)?
        }
    };
    Ok(with_start + bisector + idx as u64)
}

/// Recursively bisect the buffer to the target size between the given start and end.
fn bisect_buffer_recursive_impl<R: BufRead + Seek>(
    buf: &mut R,
    curr: &mut Vec<(u64, u64)>,
    target_size: u64,
    start: u64,
    end: u64,
) -> io::Result<()> {
    trace!(
        "bisecting buffer with target size {}, [{}, {}]",
        target_size,
        start,
        end
    );
    if end - start <= target_size {
        trace!("--> returning immediately");
        curr.push((start, end));
        return Ok(());
    }
    let bisector = bisect_buffer_with_bounds(buf, start, end)?;

    if (end - bisector) <= target_size || (bisector - start) <= target_size {
        trace!("--> returning after bisect");
        curr.push((start, bisector));
        if bisector != end {
            curr.push((bisector, end));
        }
        return Ok(());
    }

    bisect_buffer_recursive_impl(buf, curr, target_size, start, bisector)?;
    bisect_buffer_recursive_impl(buf, curr, target_size, bisector, end)?;
    Ok(())
}

/// Recursively bisect a buffer until the chunk size reaches a given boundary.
pub fn bisect_buffer_recursive<R: BufRead + Seek>(
    buf: &mut R,
    target_size: u64,
) -> io::Result<Vec<(u64, u64)>> {
    let end = buf.seek(SeekFrom::End(0))?;
    if end <= target_size * 2 {
        return Ok(vec![(0, end)]);
    }
    let cap = (end / target_size) as usize;
    let mut curr = Vec::with_capacity(cap);
    bisect_buffer_recursive_impl(buf, &mut curr, target_size, 0, end)?;
    Ok(curr)
}

/// Split a file into chunks not smaller than a given length, returning byte indices
/// for the start and end of each chunk.
pub fn chunk_file<P: AsRef<Path>>(file: P, chunk_len: u64) -> io::Result<Vec<(u64, u64)>> {
    let file = File::open(file)?;
    let mut buf = BufReader::new(file);
    bisect_buffer_recursive(&mut buf, chunk_len)
}


#[cfg(test)]
mod test {
    use super::*;
    use std::io::Cursor;
    use std::str::FromStr;

    fn cursor_from_string(s: &str) -> Cursor<String> {
        let sbuf = String::from_str(s).unwrap();
        Cursor::new(sbuf)
    }

    #[test]
    fn test_bisect_buffer_with_bounds_advances_to_newline() {
        let mut rdr = Cursor::new(b"aaa\nbbc");
        let v = bisect_buffer_with_bounds(&mut rdr, 0, 7).unwrap();
        assert_eq!(v, 3);
    }

    #[test]
    fn test_bisect_smaller_than_chunk_yields_all() {
        let mut rdr = Cursor::new(b"abcd");
        let ret = bisect_buffer_recursive(&mut rdr, 10).unwrap();
        assert_eq!(ret, vec![(0, 4)]);
    }

    #[test]
    fn test_bisect_slice_in_half() {
        let mut rdr = Cursor::new(b"aaa\nbbc");
        let ret = bisect_buffer_recursive(&mut rdr, 3).unwrap();
        assert_eq!(ret, vec![(0, 3), (3, 7)])
    }

    #[test]
    fn test_bisect_normal() {
        let mut rdr = Cursor::new(b"aaa\nbbb\nccc\nddd\n");
        let ret = bisect_buffer_recursive(&mut rdr, 3).unwrap();
        assert_eq!(ret, vec![(0, 8)]);
    }
}
