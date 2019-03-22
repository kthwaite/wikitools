use rayon::prelude::*;
use std::path::Path;
use std::sync::Mutex;
use tantivy::{
    collector::Count,
    directory::MmapDirectory,
    query::{BooleanQuery, Occur, Query, QueryParser, TermQuery},
    schema::*,
    Index, IndexReader, IndexWriter, Term,
};

use crate::page::{anchor::Anchor, PageIterator, TantivyPageIterator};
use crate::utils::open_seek_bzip;

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

    let (id, title, content, outlinks) = (
        schema.get_field("id").unwrap(),
        schema.get_field("title").unwrap(),
        schema.get_field("content").unwrap(),
        schema.get_field("outlinks").unwrap(),
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
                    let outlinks_content = page_content
                        .match_indices("[[")
                        .filter_map(|(begin, _)| Anchor::pare_anchor_match(&page_content, begin))
                        .map(Anchor::parse)
                        .map(|anchor| match anchor {
                            Anchor::Direct(name) => name.replace(" ", "_"),
                            Anchor::Label { page, .. } => page.replace(" ", "_"),
                        })
                        .collect::<Vec<_>>()
                        .join(" ");
                    doc.add_text(outlinks, &outlinks_content);
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
    outlinks: Field,
    content: Field,
    text_count_parser: QueryParser,
    out_link_parser: QueryParser,
}

impl TantivyWikiIndex {
    /// Open a Tantivy index at the given path.
    pub fn new<P: AsRef<Path>>(index_dir: P) -> Self {
        let index = {
            let mmap_dir = MmapDirectory::open(index_dir).unwrap();
            Index::open(mmap_dir).unwrap()
        };

        let reader = index.reader().unwrap();
        let schema = TantivyWikiIndex::create_schema();

        let content = schema.get_field("content").unwrap();
        let text_count_parser = QueryParser::for_index(&index, vec![content]);

        let outlinks = schema.get_field("outlinks").unwrap();
        let mut out_link_parser = QueryParser::for_index(&index, vec![outlinks]);
        out_link_parser.set_conjunction_by_default();

        TantivyWikiIndex {
            index,
            reader,
            schema,
            outlinks,
            content,
            text_count_parser,
            out_link_parser,
        }
    }

    /// Create the default schema for wikipedia data.
    ///
    /// ## Fields
    /// * `id` - Page ID; FAST
    /// * `title` - Page title; STRING | STORED
    /// * `content` - Page content; default tokenizer, indexed `WithFreqsAndPositions`.
    /// * `outlinks` - Page links; default tokenizer, indexed `WithFreqs`.
    pub fn create_schema() -> Schema {
        let mut schema_builder = Schema::builder();

        schema_builder.add_u64_field("id", FAST);
        schema_builder.add_text_field("title", STRING | STORED);
        let options = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_index_option(IndexRecordOption::WithFreqsAndPositions)
                .set_tokenizer("default"),
        );
        schema_builder.add_text_field("content", options);
        let options = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_index_option(IndexRecordOption::WithFreqs)
                .set_tokenizer("default"),
        );
        schema_builder.add_text_field("outlinks", options);

        schema_builder.build()
    }

    pub fn count_matches_for_query(&self, query: &str) -> usize {
        let query = format!(r#""{}""#, query);
        let query = self.text_count_parser.parse_query(&query).unwrap();

        self.reader.searcher().search(&*query, &Count).unwrap()
    }

    pub fn count_mutual_outlinks<S: AsRef<str>>(&self, query: &[S]) -> usize {
        // let terms: Vec<(Occur, Box<Query>)> = query
        //     .iter()
        //     .map(|term| Term::from_field_text(self.outlinks, term.as_ref()))
        //     .map(|term| {
        //         (
        //             Occur::Must,
        //             Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs))
        //                 as Box<dyn Query>
        //         )
        //     })
        //     .collect();
        // let query = BooleanQuery::from(terms);
        let query = query
            .iter()
            .map(|t| t.as_ref())
            .collect::<Vec<_>>()
            .join(" AND ");
        let query = self.out_link_parser.parse_query(&query).unwrap();
        self.index
            .reader()
            .unwrap()
            .searcher()
            .search(&query, &Count)
            .unwrap()
    }
}

                TermQuery::new(
                    Term::from_field_text(self.outlinks, term),
                    IndexRecordOption::Basic,
                );
            })
            .collect::<Vec<_>>();
        let query = BooleanQuery::new_multiterms_query(terms);
        self.search(&*query, &Count).unwrap()
    }
}
}