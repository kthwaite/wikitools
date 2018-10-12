use utils::open_bzip;

use std::collections::HashMap;
use std::path::{Path};
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};

use spinners::{Spinner, Spinners};
use pbr::ProgressBar;

pub type WikiDumpIndices = HashMap<usize, Vec<usize>>;


/// Find template indices in an index file.
pub fn find_template_indices(path: &Path) -> io::Result<WikiDumpIndices> {
    let buf = open_bzip(path)?;

    let mut hm : HashMap<usize, Vec<usize>> = HashMap::default();

    let spinner = Spinner::new(Spinners::Dots, "Finding templates...".to_owned());
    let lines = buf.lines()
                   .map(|line| line.unwrap())
                   .filter(|line| line.find("Template:").is_some())
                   .collect::<Vec<_>>();
    spinner.stop();
    let mut pbar = ProgressBar::new(lines.len() as u64);
    for line in lines {
        let pair : Vec<&str> = line.split(':').take(2).collect();
        let outer = pair[0].parse::<usize>().unwrap();
        let inner = pair[1].parse::<usize>().unwrap();
        hm.entry(outer)
          .or_insert_with(Vec::new)
          .push(inner);
        pbar.inc();
    }
    Ok(hm)
}


/// Write (Page ID, Title) pairs from an index file.
pub fn write_title_pageids<R: BufRead, W: Write>(indices: R, writer: &mut W) -> io::Result<()> {
    let mut index = 0;

    indices.lines()
            .map(|line| line.unwrap())
            .filter_map(|line| {
                index += 1;
                if index % 100_000 == 0 {
                    println!("Processed {} indices", index);
                }
                if let Some(index) = line.find(':') {
                    let pair = &line[index+1..];
                    if let Some(index) = pair.find(':') {
                        let pair = (pair[0..index].to_owned(), pair[index+1..].to_owned());
                        return Some(pair);
                    }
                    return None;
                }
                None
            })
            .for_each(|(index, title)| writeln!(writer, "{}\t{}", index, title).unwrap());
    Ok(())
}

/// Build a lookup table of all indices.
pub fn build_indices_map(path: &Path) -> io::Result<WikiDumpIndices> {
    let indices = open_bzip(path)?;

    let hm : WikiDumpIndices = HashMap::default();

    let mut counter = 0;

    let hm = indices.lines()
           .map(|line| {
               line.unwrap()
           })
           .map(|line| {
                let pair : Vec<&str> = line.split(':').take(2).collect();
                let outer = pair[0].parse::<usize>().unwrap();
                let inner = pair[1].parse::<usize>().unwrap();
                counter += 1;
                if counter % 100_000 == 0 {
                    println!("Read {} lines", counter);
                }
                (outer, inner)
           })
           .fold(hm, |mut hm, (o, i)| {
                hm.entry(o)
                  .or_insert_with(Vec::new)
                  .push(i);
                hm
           });
    Ok(hm)
}


/// Read an indices file.
pub fn read_indices(path: &Path) -> io::Result<WikiDumpIndices> {
    let file = File::open(path)?;
    let buf = BufReader::new(file);
    let hm : HashMap<usize, Vec<usize>> = HashMap::default();
    let hm = buf.lines()
       .map(|line| line.unwrap())
       .map(|line| {
                let pair : Vec<&str> = line.split(' ').take(2).collect();
                let outer = pair[0].parse::<usize>().unwrap();
                let inner = pair[1].split(',').map(|num| num.parse::<usize>().unwrap()).collect::<Vec<_>>();
                (outer, inner)
           })
           .fold(hm, |mut hm, (o, i)| {
                hm.insert(o, i);
                hm
           });
    Ok(hm)
}


/// Write a HashMap of indices to file.
pub fn write_indices(hs: &WikiDumpIndices, path: &Path) -> io::Result<()> {
    let out = File::create(path)?;
    let mut buf = BufWriter::with_capacity(8192 * 4, out);
    for (outer, inners) in hs.iter() {
        write!(&mut buf, "{} ", outer);
        inners[0..inners.len() - 1].iter().for_each(|inner| {
            write!(&mut buf, "{},", inner);
        });
        writeln!(&mut buf, "{}", inners.last().unwrap());
    }
    Ok(())
}


/// Fetch and write the indices of each Template.
pub fn write_template_indices(index: &Path, output: &Path) -> WikiDumpIndices {
    let hx = find_template_indices(index).unwrap();
    write_indices(&hx, output).unwrap();
    hx
}


/// Fetch and write all indices.
pub fn write_all_indices(index: &Path, out_path: &Path) -> WikiDumpIndices {
    let hx = build_indices_map(index).unwrap();
    write_indices(&hx, out_path).unwrap();
    hx
}
