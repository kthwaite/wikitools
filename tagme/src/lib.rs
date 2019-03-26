#![allow(dead_code, unused_imports)]
pub mod query;
pub mod query_text;
pub mod stopwords;
pub mod tag_me;
pub mod params;

pub use crate::tag_me::TagMe;
pub use crate::query::TagMeQuery;
pub use crate::params::TagMeParams;