use tantivy::{
    Index,
    IndexWriter,
    IndexReader,
    schema::*,
    directory::MmapDirectory,
    collector::Count,
    query::QueryParser,
};
use env_logger;
use log::{info, debug};
use std::sync::Mutex;
use std::path::Path;
use rayon::prelude::*;

use crate::utils::{open_seek_bzip};
use crate::page::{PageIterator, TantivyPageIterator};


/// Create the default schema for wikipedia data.
///
/// ## Fields
/// * `id` - Page ID; FAST
/// * `title` - Page title; STRING | STORED
/// * `content` - Page content; default tokenizer, indexed `WithFreqsAndPositions`.
pub fn create_schema() -> Schema {
    let mut schema_builder = Schema::builder();

    schema_builder.add_u64_field("id", FAST);
    schema_builder.add_text_field("title", STRING | STORED);
    let options = TextOptions::default()
        .set_indexing_options(
            TextFieldIndexing::default()
                .set_index_option(IndexRecordOption::WithFreqsAndPositions)
                .set_tokenizer("default")
        );
    schema_builder.add_text_field("content", options);

    schema_builder.build()
}

/// Use tantivy to index content from a bzip2 multistream.
///
/// This index is used to fetch title and category data, and for determining document
/// frequency for a given surface form in the query pruning process.
pub fn index_anchors(
    mut indices: Vec<&usize>,
    data_path: &Path,
    indexer: &Mutex<IndexWriter>,
    schema: &Schema,
) -> Result<(), Box<std::error::Error>> {
    let pbar = Mutex::new(pbr::ProgressBar::new(indices.len() as u64));
    indices.sort();

    let (id, title, content) = (
        schema.get_field("id").unwrap(),
        schema.get_field("title").unwrap(),
        schema.get_field("content").unwrap(),
    );

    indices
        .into_par_iter()
        .map(|index| {
            let store = open_seek_bzip(data_path, *index).unwrap();
            TantivyPageIterator(PageIterator::new(store))
                .map(|(page_id, page_title, page_content)| {
                    let mut doc = Document::default();
                    doc.add_u64(id, page_id.parse::<u64>().unwrap());
                    doc.add_text(title, &page_title);
                    doc.add_text(content, &page_content);
                    doc
                })
                .collect::<Vec<_>>()
        })
        .for_each(|docs| {
            {
                let mut index_writer = indexer.lock().expect("Failed to unlock indexer");
                for doc in docs {
                    index_writer.add_document(doc);
                }
            }
            {
                let mut prog_bar = pbar.lock().expect("Failed to unlock progress-bar");
                prog_bar.inc();
            }
        });
    Ok(())
}

pub struct TantivyWikiIndex {
    index: Index,
    reader: IndexReader,
    schema: Schema,
    query_parser: QueryParser,
}

impl TantivyWikiIndex {
    /// Open a Tantivy index at the given path.
    pub fn new<P: AsRef<Path>>(index_dir: P) -> Self {
        let index = {
            let mmap_dir = MmapDirectory::open(index_dir).unwrap();
            Index::open(mmap_dir).unwrap()
        };

        let reader = index.reader().unwrap();
        let schema = create_schema();
        let content = schema.get_field("content").unwrap();
        let query_parser = QueryParser::for_index(&index, vec![content]);

        TantivyWikiIndex {
            index,
            reader,
            schema,
            query_parser,
        }
    }

    pub fn count_matches_for_query(&self, query: &str) -> usize {
        // let query_parser = QueryParser::for_index(&self.index, vec![self.content]);
        let query = format!(r#""{}""#, query);
        let query = self.query_parser.parse_query(&query).unwrap();

        self.reader.searcher().search(&*query, &Count).unwrap() 
    }
}