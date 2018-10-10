use utils::open_bzip;

use std::collections::HashMap;
use std::path::{Path};
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};

use spinners::{Spinner, Spinners};
use pbr::ProgressBar;


/// Find template indices in an index file.
pub fn find_template_indices(path: &Path) -> io::Result<HashMap<usize, Vec<usize>>> {
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
          .or_insert(Vec::new())
          .push(inner);
        pbar.inc();
    }
    Ok(hm)
}

/// Build a lookup table of all indices.
pub fn build_indices_map(path: &Path) -> io::Result<HashMap<usize, Vec<usize>>> {
    let indices = open_bzip(path)?;

    let hm : HashMap<usize, Vec<usize>> = HashMap::default();

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
                if counter % 100000 == 0 {
                    println!("Read {} lines", counter);
                }
                (outer, inner)
           })
           .fold(hm, |mut hm, (o, i)| {
                hm.entry(o)
                  .or_insert(Vec::new())
                  .push(i);
                hm
           });
    Ok(hm)
}

/// Read an indices file.
pub fn read_indices(path: &Path) -> io::Result<HashMap<usize, Vec<usize>>> {
    let file = File::open(path)?;
    let buf = BufReader::new(file);
    let hm : HashMap<usize, Vec<usize>> = HashMap::default();
    let hm = buf.lines()
       .map(|line| line.unwrap())
       .map(|line| {
            let pair : Vec<&str> = line.split(':').take(2).collect();
            let outer = pair[0].parse::<usize>().unwrap();
            let inner = pair[1].parse::<usize>().unwrap();

                (outer, inner)
           })
           .fold(hm, |mut hm, (o, i)| {
                hm.entry(o)
                  .or_insert(Vec::new())
                  .push(i);
                hm
           });
    Ok(hm)
}

/// Write a HashMap of indices to file.
pub fn write_indices(hs: &HashMap<usize, Vec<usize>>, path: &Path) -> io::Result<()> {
    let out = File::create(path)?;
    let mut buf = BufWriter::with_capacity(8192 * 4, out);
    for (outer, inners) in hs.iter() {
        write!(&mut buf, "{} ", outer);
        inners[0..inners.len() - 1].iter().for_each(|inner| {
            write!(&mut buf, "{},", inner);
        });
        write!(&mut buf, "{}\n", inners.last().unwrap());
    }
    Ok(())
}

/// Fetch and write the indices of each Template.
pub fn write_template_indices(index: &Path, output: &Path) {
    let hx = find_template_indices(index).unwrap();
    write_indices(&hx, output).unwrap();
}

/// Fetch and write all indices.
pub fn write_all_indices(index: &Path, out_path: &Path) {
    let hx = build_indices_map(index).unwrap();
    write_indices(&hx, out_path).unwrap();
}
