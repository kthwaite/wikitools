use tagme::TagMeQuery;

use log::info;
use tantivy::{directory::MmapDirectory, schema::*, Index};

use storage::fst::WikiAnchors;
use storage::tantivy::TantivyWikiIndex;

pub fn create_schema() -> Schema {
    let mut schema_builder = SchemaBuilder::default();

    schema_builder.add_u64_field("id", FAST);
    schema_builder.add_text_field("title", STRING | STORED);
    schema_builder.add_text_field("content", TEXT);

    schema_builder.build()
}

fn main() {
    use env_logger;
    env_logger::init();
    info!("Running query");

    let map = WikiAnchors::new("./anchors-flat.fst").unwrap();
    let index = TantivyWikiIndex::new("./wiki-index");

    let mut qry = TagMeQuery::new("Orpheus and Eurydice", 1.0);
    let ents = qry.parse(&map, &index);
    let ents = qry.disambiguate(&ents);
    println!("{:?}", ents);
}
