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
use crate::tokenizer::WikiTitleTokenizer;

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
    doc_count: usize,
}

impl TantivyWikiIndex {
    /// Open a Tantivy index at the given path.
    pub fn new<P: AsRef<Path>>(index_dir: P) -> Self {
        let index = TantivyWikiIndex::load_index(&index_dir);
        TantivyWikiIndex::from_index(index)
    }

    /// Create from an index.
    pub fn from_index(index: Index) -> Self {
        let reader = index.reader().unwrap();
        let schema = TantivyWikiIndex::create_schema();

        let content = schema.get_field("content").unwrap();
        let text_count_parser = QueryParser::for_index(&index, vec![content]);

        let outlinks = schema.get_field("outlinks").unwrap();
        let mut out_link_parser = QueryParser::for_index(&index, vec![outlinks]);
        out_link_parser.set_conjunction_by_default();

        let doc_count = reader.searcher().search(&AllQuery, &Count).unwrap();

        TantivyWikiIndex {
            index,
            reader,
            schema,
            outlinks,
            content,
            text_count_parser,
            out_link_parser,
            doc_count,
        }
    }

    /// Load an index from the given directory.
    pub fn load_index<P: AsRef<Path>>(index_dir: &P) -> Index {
        let index = {
            let mmap_dir = MmapDirectory::open(index_dir).unwrap();
            Index::open(mmap_dir).unwrap()
        };
        TantivyWikiIndex::configure_index(index)
    }

    pub fn configure_index(index: Index) -> Index {
        index.tokenizers().register("wiki", WikiTitleTokenizer);
        index
    }

    /// Load or create an index in the given directory.
    pub fn load_or_create_index<P: AsRef<Path>>(index_dir: &P) -> Index {
        let schema = TantivyWikiIndex::create_schema();
        let index = match MmapDirectory::open(index_dir) {
            Ok(mmap_dir) => {
                if Index::exists(&mmap_dir) {
                    Index::open(mmap_dir).unwrap()
                } else {
                    Index::create_in_dir(index_dir, schema).unwrap()
                }
            }
            _ => Index::create_in_dir(index_dir, schema).unwrap(),
        };
        TantivyWikiIndex::configure_index(index)
    }

    /// Get the number of documents in the index.
    pub fn len(&self) -> usize {
        self.doc_count
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
                .set_tokenizer("wiki"),
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
        let terms: Vec<(Occur, Box<Query>)> = query
            .iter()
            .map(|term| Term::from_field_text(self.outlinks, term.as_ref()))
            .map(|term| {
                (
                    Occur::Must,
                    Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs))
                        as Box<dyn Query>
                )
            })
            .collect();
        let query = BooleanQuery::from(terms);
        self.reader
            .searcher()
            .search(&query, &Count)
            .unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tantivy::{
        query::{BooleanQuery, Occur, Query, TermQuery},
        Term,
    };

    fn create_ram_index(schema: &Schema) -> (Index, IndexWriter) {
        let index = Index::create_in_ram(schema.clone());
        let index = TantivyWikiIndex::configure_index(index);
        let writer = index.writer(10_000_000).unwrap();
        (index, writer)
    }

    fn get_schema_fields(schema: &Schema) -> (Field, Field, Field, Field) {
        (
            schema.get_field("id").unwrap(),
            schema.get_field("title").unwrap(),
            schema.get_field("content").unwrap(),
            schema.get_field("outlinks").unwrap(),
        )
    }

    fn as_term_query(field: Field, query: &str) -> Box<dyn Query> {
        let term = Term::from_field_text(field, query);
        Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs))
    }

    #[test]
    fn test_build_index_query() {
        let schema = TantivyWikiIndex::create_schema();
        let (index, mut writer) = create_ram_index(&schema);
        let (id, title, content, outlinks) = get_schema_fields(&schema);

        let mut doc = Document::default();
        doc.add_u64(id, 0);
        doc.add_text(title, "Spider");
        doc.add_text(content, "This is a page about spiders.");
        doc.add_text(
            outlinks,
            "Arachnids Insects Famous_Spiders The_Famous_Spiders_(band)",
        );
        writer.add_document(doc);

        let mut doc = Document::default();
        doc.add_u64(id, 1);
        doc.add_text(title, "The_Louvre");
        doc.add_text(content, "The Louvre is a famous museum run by insects.");
        doc.add_text(
            outlinks,
            "Insects The_Famous_Spiders_(band) Leopold_Poussin",
        );
        writer.add_document(doc);

        writer.commit().unwrap();

        let reader = index.reader().unwrap();
        let doc_count = reader.searcher().search(&AllQuery, &Count).unwrap();
        assert_eq!(doc_count, 2);

        let out_link_parser = QueryParser::for_index(&index, vec![outlinks]);

        // Query parser will mangle this!
        let query: Box<dyn Query> = as_term_query(outlinks, "Famous_Spiders");
        let doc_count = reader.searcher().search(&query, &Count).unwrap();
        assert_eq!(doc_count, 1);

        // No underscores, OK.
        let query = out_link_parser
            .parse_query("Insects AND Arachnids")
            .unwrap();
        let doc_count = reader.searcher().search(&query, &Count).unwrap();
        assert_eq!(doc_count, 1);

        // Underscores, not OK!
        let query = out_link_parser
            .parse_query("Insects AND Famous_Spiders")
            .unwrap();
        let doc_count = reader.searcher().search(&query, &Count).unwrap();
        // Tantivy query parser behavioiur changed if this fails.
        assert_eq!(doc_count, 0);

        let query = BooleanQuery::from(vec![
            (Occur::Must, as_term_query(outlinks, "Famous_Spiders")),
            (Occur::Must, as_term_query(outlinks, "Insects")),
        ]);
        let doc_count = reader.searcher().search(&query, &Count).unwrap();
        assert_eq!(doc_count, 1);

        let query = BooleanQuery::from(vec![
            (Occur::Must, as_term_query(outlinks, "Arachnids")),
            (Occur::Must, as_term_query(outlinks, "Insects")),
        ]);
        let doc_count = reader.searcher().search(&query, &Count).unwrap();
        assert_eq!(doc_count, 1);

        let query = BooleanQuery::from(vec![
            (
                Occur::Must,
                as_term_query(outlinks, "The_Famous_Spiders_(band)"),
            ),
            (Occur::Must, as_term_query(outlinks, "Insects")),
        ]);
        let doc_count = reader.searcher().search(&query, &Count).unwrap();
        assert_eq!(doc_count, 2);
    }
}
