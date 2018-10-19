pub struct FindIndices<'a> {
    haystack: &'a str,
    needle: String,
    offset: usize
}

impl<'a> FindIndices<'a> {
    pub fn new(haystack: &'a str, needle: &str) -> Self {
        FindIndices {
            haystack,
            needle: String::from(needle),
            offset: 0
        }
    }
}

impl<'a> Iterator for FindIndices<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        self.haystack.find(&self.needle)
                     .and_then(|index| {
                        self.haystack = &self.haystack[index + 1..];
                        self.offset += index + 1;
                        Some(self.offset - 1)
                     })
    }
}

pub trait IndicesOf {
    fn indices_of<'a>(&'a self, needle: &str) -> FindIndices<'a>;
}

impl IndicesOf for str {
    /// An iterator over the indices for matches of a pattern within a string
    /// slice.
    fn indices_of<'a>(&'a self, needle: &str) -> FindIndices<'a> {
        FindIndices::new(self, needle)
    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_indices_of() {
        let idx = "abaabaaaababbbaaaababbaab".indices_of("b")
                                             .collect::<Vec<_>>();
        assert_eq!(idx, vec![1,4,9,11,12,13,18,20,21,24]);
    }
}
