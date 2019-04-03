use vecdump::word2vec::{Word2VecMmap, Word2VecStore};


fn main() -> Result<(), Box<std::error::Error>> {
    let w2v = Word2VecMmap::load("./data/enwiki_20180420_300d")?;
    let ret = w2v.get("anarchism").unwrap();
    println!("{:?}", ret);
    let ret = w2v.get("ENTITY/Anarchism").unwrap();
    println!("{:?}", ret);
    Ok(())
}
