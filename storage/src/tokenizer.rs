use tantivy::tokenizer::{Token, TokenStream, Tokenizer};
use std::str::CharIndices;

/// Tokenize wiki titles, based on tantivy::tokenizer::SimpleTokenizer.
#[derive(Clone)]
pub struct WikiTitleTokenizer;

pub struct WikiTokenStream<'a> {
    text: &'a str,
    chars: CharIndices<'a>,
    token: Token,
}

impl<'a> Tokenizer<'a> for WikiTitleTokenizer {
    type TokenStreamImpl = WikiTokenStream<'a>;

    fn token_stream(&self, text: &'a str) -> Self::TokenStreamImpl {
        WikiTokenStream {
            text,
            chars: text.char_indices(),
            token: Token::default(),
        }
    }
}


impl<'a> WikiTokenStream<'a> {
    // search for the end of the current token.
    fn search_token_end(&mut self) -> usize {
        (&mut self.chars)
            .filter(|&(_, ref c)| c.is_whitespace())
            .map(|(offset, _)| offset)
            .next()
            .unwrap_or_else(|| self.text.len())
    }
}

impl<'a> TokenStream for WikiTokenStream<'a> {
    fn advance(&mut self) -> bool {
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);

        loop {
            match self.chars.next() {
                Some((offset_from, c)) => {
                    if c.is_alphanumeric() {
                        let offset_to = self.search_token_end();
                        self.token.offset_from = offset_from;
                        self.token.offset_to = offset_to;
                        self.token.text.push_str(&self.text[offset_from..offset_to]);
                        return true;
                    }
                }
                None => {
                    return false;
                }
            }
        }
    }

    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}