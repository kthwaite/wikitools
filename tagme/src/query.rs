use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
    static ref ILLEGAL_CHARS: Regex = Regex::new(r#"[^A-Za-z0-9]+"#).unwrap();
    static ref MULTI_WS: Regex = Regex::new(r#"\r|\n|\r\n|\s{2,}"#).unwrap();
}

/// Return n-grams over an input string.
pub fn get_ngrams(input: &str, n: usize) -> Vec<&str> {
    if n == 0 {
        return vec![];
    }

    let indices: Vec<usize> = vec![(0, ' ')]
        .into_iter()
        .chain(input.char_indices())
        .filter(|(_, c)| c == &' ')
        .map(|(i, _)| i)
        .collect();

    if n > indices.len() {
        return vec![];
    }

    indices
        .iter()
        .zip(indices.iter().skip(n).chain([input.len()].iter()))
        .map(|(begin, end)| &input[*begin..*end])
        .collect()
}

#[derive(Clone, Debug)]
pub struct Query {
    qid: usize,
    query: String,
}

impl Query {
    pub fn new(qid: usize, query: &str) -> Self {
        let query = Query::preprocess(query);
        Query { qid, query }
    }

    /// Preprocess a query, removing special characters
    pub fn preprocess(input: &str) -> String {
        let input = ILLEGAL_CHARS.replace(input, " ");
        MULTI_WS.replace(&input, " ").to_lowercase()
    }

    /// Split the query into constituent n-grams.
    pub fn split_ngrams(&self) -> Vec<&str> {
        let mut indices: Vec<usize> = vec![(0, ' ')]
            .into_iter()
            .chain(self.query.char_indices())
            .filter(|(_i, c)| c == &' ')
            .map(|(i, _)| i)
            .collect();
        indices.push(self.query.len());
        (1..indices.len())
            .flat_map(|n| {
                indices
                    .iter()
                    .zip(indices.iter().skip(n))
                    .map(|(begin, end)| self.query[*begin..*end].trim())
            })
            .collect()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_ngrams() {
        let qx = get_ngrams("The quick brown fox", 2);
        assert_eq!(vec!["The quick", " quick brown", " brown fox"], qx);

        let qx = get_ngrams("The quick brown fox", 10);
        assert!(qx.is_empty());
    }

    #[test]
    fn test_query_ngrams() {
        let q = Query::new(0, "The quick brown fox");
        let split_ngrams = vec![
            "the",
            "quick",
            "brown",
            "fox",
            "the quick",
            "quick brown",
            "brown fox",
            "the quick brown",
            "quick brown fox",
            "the quick brown fox",
        ];
        assert_eq!(split_ngrams, q.split_ngrams());
    }
}
