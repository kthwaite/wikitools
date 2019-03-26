use crate::query_text::Query;
use crate::stopwords::STOPWORDS_EN;
use super::params::TagMeParams;
use log::{debug, info, trace, error};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use storage::fst::WikiAnchors;
use storage::surface_form::{SurfaceForm, SurfaceFormStoreRead};
use storage::tantivy::TantivyWikiIndex;


pub struct TagMe<S: SurfaceFormStoreRead> {
    pub params: TagMeParams,
    pub surface_forms: S,
    pub wiki_index: TantivyWikiIndex,
}

impl<S: SurfaceFormStoreRead> TagMe<S> {
    pub fn new(surface_forms: S, wiki_index: TantivyWikiIndex) -> Self {
        TagMe::with_params(Default::default(), surface_forms, wiki_index)
    }

    pub fn with_params(params: TagMeParams, surface_forms: S, wiki_index: TantivyWikiIndex) -> Self {
        TagMe {
            params,
            surface_forms,
            wiki_index,
        }
    }

    /// Set the parameters.
    pub fn set_params(&mut self, params: TagMeParams) {
        self.params = params;
    }
    /// Get the link probability for a given mention.
    ///
    /// From the paper:
    ///     For a mention `m` this is calculated as key(m) / df(m), where
    ///     key(m) denotes number of Wikipedia articles where `m` is
    ///     selected as a keyword, i.e., linked to an entity (any entity),
    ///     and df(m) is the number of articles containing the mention.
    ///
    pub fn get_link_probability(&self, mention: &SurfaceForm) -> f32 {
        let mention_freq = self.wiki_index.count_matches_for_query(&mention.text) as f32;
        trace!("mention freq `{}` == {}", &mention.text, mention_freq);
        if mention_freq == 0.0 {
            return 0.0;
        }
        let wiki_occurrences = mention.wiki_occurrences();
        // This is TAGME implementation, from source code:
        // link_probability = float(wiki_occurrences) / max(mention_freq, wiki_occurrences)
        let ret = wiki_occurrences / (mention_freq).max(wiki_occurrences);
        trace!("calculated link prob == {}", ret);
        ret
    }

    /// Get the entities and corresponding link probabilities for a query
    pub fn entities_for_query(&self, query: &Query) -> (HashMap<String, HashMap<String, f32>>, HashMap<String, f32>) {
        let mut entities: HashMap<String, HashMap<String, f32>> = Default::default();
        let mut link_probabilities: HashMap<String, f32> = Default::default();
        for ngram in query.split_ngrams().into_iter().filter(|ngram| {
            ngram.split(' ').any(|tok| !STOPWORDS_EN.contains(&tok))
        }) {
            let w_count = ngram.matches(' ').count() + 1;
            if w_count < self.params.ngram_min || w_count > self.params.ngram_max {
                continue;
            }
            info!("candidate phrase: {}", ngram);
            let mention = match self.surface_forms.get(ngram) {
                Ok(Some(mention)) => mention,
                Ok(None) => continue,
                Err(err) => {
                    error!("{}", err);
                    continue
                },
            };
            // TODO: this should be configurable.
            if mention.wiki_occurrences() < 2.0 {
                continue;
            }

            let link_probability = self.get_link_probability(&mention);
            if link_probability < self.params.link_probability_threshold {
                continue;
            }
            trace!("NGRAM: {} (p-link={})", ngram, link_probability);

            link_probabilities
                .insert(ngram.to_string(), link_probability as f32);

            // TODO: according to Faegheh Hasibi's implementation, this
            // threshold was only in the TAGME source; needs tuning.
            entities.insert(
                ngram.to_owned(),
                mention.get_wiki_matches(self.params.candidate_mention_threshold),
            );
        }
        (entities, link_probabilities)
    }

    /// Get the top K percent of mentions by score.
    pub fn get_top_k<'a>(&self, mention_scores: &'a HashMap<String, f32>) -> Vec<&'a str> {
        let mention_scores_count = mention_scores.len();
        if mention_scores_count == 1 {
            return mention_scores
                .keys()
                .map(|k| k.as_str())
                .collect::<Vec<&str>>();
        }
        let k: usize = {
            let k = ((mention_scores_count as f32) * self.params.k_th).round() as usize;
            k.min(1)
        };

        let mut sorted_scores: Vec<(&str, &f32)> = mention_scores
            .iter()
            .map(|(s, v)| (s.as_str(), v))
            .collect();
        sorted_scores.sort_by(|(_, score0), (_, score1)| score1.partial_cmp(score0).unwrap());
        let mut top_k_ens = vec![];
        let mut count = 1;

        let mut prev_rel_score = sorted_scores[0].1;
        for (en, rel_score) in sorted_scores {
            if (rel_score - prev_rel_score).abs() > std::f32::EPSILON {
                count += 1
            }
            if count > k {
                break;
            }
            top_k_ens.push(en);
            prev_rel_score = rel_score;
        }
        top_k_ens
    }
}
