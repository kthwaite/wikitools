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

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_tokenize_one() {
        let tokz = WikiTitleTokenizer;
        let mut stream = tokz.token_stream("Nicolas_Poussin");
        assert_eq!(stream.token().text, "");
        assert!(stream.advance());
        assert_eq!(stream.token().text, "Nicolas_Poussin");
        assert!(!stream.advance());
    }
    #[test]
    fn test_tokenize_two() {
        let tokz = WikiTitleTokenizer;
        let mut stream = tokz.token_stream("Nicolas_Poussin The_Louvre");
        assert_eq!(stream.token().text, "");
        assert!(stream.advance());
        assert_eq!(stream.token().text, "Nicolas_Poussin");
        assert!(stream.advance());
        assert_eq!(stream.token().text, "The_Louvre");
        assert!(!stream.advance());
    }

    #[test]
    fn test_tokenize_many() {
        let tokz = WikiTitleTokenizer;
        let input = &[
            "Nicolas_Poussin",
            "The_Louvre",
            "Orpheus_and_Eurydice",
            "The_Unattainable_Object_of_Desire",
            "Paris,_Texas",
            "The_Louvre_(song)",
            "Local_Newspaper's_(band)",
            "Joan_of_Arc",
        ];
        let input_str = input.join(" ");
        let mut stream = tokz.token_stream(&input_str);
        assert_eq!(stream.token().text, "");    
        for token in input {
            assert!(stream.advance());
            assert_eq!(stream.token().text, *token);
        }
        assert!(!stream.advance());
    }
}