#![allow(dead_code, unused_imports)]
pub mod query;
pub mod stopwords;

use crate::query::Query;
use crate::stopwords::STOPWORDS_EN;
use log::{debug, info, trace};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use storage::fst::WikiAnchors;
use storage::surface_form::SurfaceForm;
use storage::tantivy::TantivyWikiIndex;

/// Hash an arbitrary slice of str.
fn hash_str_slice<S: AsRef<str> + Hash>(en_uris: &[S]) -> u64 {
    let mut s = DefaultHasher::new();
    for uri in en_uris {
        uri.hash(&mut s);
    }
    s.finish()
}

#[derive(Clone, Copy, Debug)]
pub struct TagMeParams {
    link_probability_threshold: f32,
    candidate_mention_threshold: f32,
    k_th: f32,
}

impl Default for TagMeParams {
    fn default() -> Self {
        TagMeParams {
            link_probability_threshold: 0.001,
            candidate_mention_threshold: 0.02,
            k_th: 0.3,
        }
    }
}

#[derive(Debug)]
pub struct TagMeQuery {
    query: Query,
    rho_th: f32,

    params: TagMeParams,

    link_probabilities: HashMap<String, f32>,
    in_links: HashMap<u64, usize>,
    rel_scores: HashMap<String, HashMap<String, f32>>, // dictionary {men: {en: rel_score, ...}, ...}
    disamb_ens: HashMap<String, usize>,
}

impl TagMeQuery {
    pub fn new(query_str: &str, rho_th: f32) -> Self {
        TagMeQuery {
            query: Query::new(0, query_str),
            rho_th,

            params: TagMeParams::default(),

            link_probabilities: HashMap::default(),
            in_links: HashMap::default(),
            rel_scores: HashMap::default(),
            disamb_ens: HashMap::default(),
        }
    }

    /// Get the link probability for a given mention.
    ///
    /// From the paper:
    ///     For a mention `m` this is calculated as key(m) / df(m), where
    ///     key(m) denotes number of Wikipedia articles where `m` is
    ///     selected as a keyword, i.e., linked to an entity (any entity),
    ///     and df(m) is the number of articles containing the mention.
    ///
    fn get_link_probability(&self, wiki_index: &TantivyWikiIndex, mention: &SurfaceForm) -> f32 {
        // pq = ENTITY_INDEX.get_phrase_query(mention.text, Lucene.FIELDNAME_CONTENTS)
        // mention_freq = ENTITY_INDEX.searcher.search(pq, 1).totalHits
        let mention_freq = wiki_index.count_matches_for_query(&mention.text) as f32;
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

    ///
    /// Hasibi et al.:
    ///     To detect entity mentions, TAGME matches all n-grams of the input text,
    ///     up to n = 6, against the surface form dictionary. For an n-gram contained
    ///     by another one, TAGME drops the shorter n-gram, if it has lower link
    ///     probability than the longer one. The output of this step is a set of
    ///     mentions with their corresponding candidate entities.
    pub fn parse(
        &mut self,
        map: &WikiAnchors,
        wiki_index: &TantivyWikiIndex,
    ) -> HashMap<String, HashMap<String, f32>> {
        let mut entities: HashMap<String, HashMap<String, f32>> = HashMap::new();
        for ngram in self.query.split_ngrams().into_iter().filter(|ngram| {
            ngram.split(' ').any(|tok| !STOPWORDS_EN.contains(&tok))
            // !STOPWORDS_EN.contains(ngram)
        }) {
            let w_count = ngram.matches(' ').count() + 1;
            // TODO: this should be configurable.
            if w_count == 1 || w_count > 6 {
                continue;
            }
            trace!("NGRAM: {}", ngram);
            let mention = map.entities_for_query(ngram).unwrap();
            // TODO: this should be configurable.
            if mention.wiki_occurrences() < 2.0 {
                continue;
            }
            let link_probability = self.get_link_probability(&wiki_index, &mention);
            if link_probability < self.params.link_probability_threshold {
                continue;
            }

            // These mentions will be kept
            self.link_probabilities
                .insert(ngram.to_string(), link_probability as f32);

            // TODO: according to Faegheh Hasibi's implementation, this
            // threshold was only in the TAGME source; needs tuning.
            entities.insert(ngram.to_owned(), mention.get_wiki_matches(0.001));
        }
        trace!("entities: {:?}", entities);
        // filters containment mentions (based on paper)
        // sorts by mention length
        let mut sorted_mentions = entities.keys().cloned().collect::<Vec<_>>();

        sorted_mentions.sort_by(|k0, k1| {
            let c0 = k0.matches(' ').count() + 1;
            let c1 = k1.matches(' ').count() + 1;
            c0.partial_cmp(&c1).unwrap()
        });

        for i in 0..sorted_mentions.len() {
            let m_i = &sorted_mentions[i];
            let mut ignore_m_i = false;
            for j in i + 1..sorted_mentions.len() {
                let m_j = &sorted_mentions[j];
                if m_j.find(m_i).is_some()
                    && (self.link_probability_of(m_i) < self.link_probability_of(m_j))
                {
                    ignore_m_i = true;
                    break;
                }
            }
            if ignore_m_i {
                entities.remove(m_i);
            }
        }
        entities
    }

    fn link_probability_of(&self, sf: &str) -> f32 {
        *self.link_probabilities.get(sf).unwrap_or(&1.0)
    }

    /// vote_e = sum_e_i(mw_rel(e, e_i) * cmn(e_i)) / i
    fn get_vote(
        &mut self,
        wiki_index: &TantivyWikiIndex,
        entity: &str,
        men_cand_ens: &HashMap<String, f32>,
    ) -> f32 {
        trace!("get_vote({}, {:?})", entity, men_cand_ens);
        let mut vote: f32 = 0.0;
        for (e_i, cmn) in men_cand_ens.iter() {
            let mw_rel = self.get_mw_rel(wiki_index, entity, e_i);
            trace!("\t{} cmn:{} mw_rel:{}", e_i, cmn, mw_rel);
            vote += cmn * mw_rel;
        }
        let vote: f32 = (vote as f32) / (men_cand_ens.len() as f32);
        trace!("vote for {} -> {}", entity, vote);
        vote
    }

    /// Performs disambiguation and link each mention to a single entity.
    pub fn disambiguate(
        &mut self,
        wiki_index: &TantivyWikiIndex,
        candidate_entities: &HashMap<String, HashMap<String, f32>>,
    ) -> HashMap<String, String> {
        let mut rel_scores: HashMap<String, HashMap<String, f32>> = Default::default();
        for mention_i in candidate_entities.keys() {
            rel_scores.insert(mention_i.to_string(), HashMap::default());
            for entity_mention_i in candidate_entities.get(mention_i).unwrap().keys() {
                let vote_sum = candidate_entities.keys().fold(0.0f32, |acc, mention_j| {
                    if mention_i == mention_j {
                        return acc;
                    }
                    acc + self.get_vote(
                        wiki_index,
                        entity_mention_i,
                        &candidate_entities.get(mention_j).unwrap(),
                    )
                });
                rel_scores
                    .get_mut(mention_i)
                    .unwrap()
                    .insert(entity_mention_i.to_string(), vote_sum);
            }
        }
        trace!("rel_scores: {:?}", rel_scores);

        // pruning uncommon entities (based on the paper)
        for mention_i in rel_scores.keys() {
            for entity_mention_i in rel_scores[mention_i].keys() {
                let candidate_mention = candidate_entities[mention_i][entity_mention_i];
                if candidate_mention >= self.params.candidate_mention_threshold {
                    self.rel_scores
                        .entry(mention_i.to_string())
                        .or_insert_with(Default::default)
                        .insert(
                            entity_mention_i.to_string(),
                            rel_scores[mention_i][entity_mention_i],
                        );
                }
            }
        }

        // DT pruning
        let mut disambiguated_entities: HashMap<String, String> = Default::default();
        for mention_i in self.rel_scores.keys() {
            trace!("evaluating mention {}", mention_i);
            if self.rel_scores[mention_i].len() == 0 {
                trace!("skipping mention {}, score zero", mention_i);
                continue;
            }
            let top_k_entities = self.get_top_k(mention_i);
            trace!("top_k_entities for {}: {:?}", mention_i, top_k_entities);
            let mut best_cmn = 0.0f32;
            let mut best_en: Option<&str> = None;
            for entity in top_k_entities {
                let cmn = candidate_entities
                    .get(mention_i)
                    .unwrap()
                    .get(entity)
                    .unwrap();
                if *cmn >= best_cmn {
                    best_en = Some(&entity);
                    best_cmn = *cmn;
                }
            }
            disambiguated_entities.insert(
                mention_i.to_string(),
                best_en.unwrap_or("[-ERROR-]").to_string(),
            );
        }
        disambiguated_entities
    }

    fn prune(&self, _disambiguated_entities: HashMap<String, HashMap<String, f32>>) {}

    fn process(&self) {
        // let candidate_entities = self.parse();
        // let disambiguated_entities = self.disambiguate(candidate_entities);
        // let pruned = self.prune(disambiguated_entities);
    }

    /// Calculates Milne & Witten relatedness for two entities.
    /// This implementation is based on Hasibi's implementation, which in turn
    /// is based on the 'Dexter' implementation (which is similar to TAGME implementation).
    /// - Dexter implementation: https://github.com/dexter/dexter/blob/master/dexter-core/src/main/java/it/cnr/isti/hpc/dexter/relatedness/MilneRelatedness.java
    /// - TAGME: it.acubelab.tagme.preprocessing.graphs.OnTheFlyArrayMeasure
    fn get_mw_rel(&mut self, wiki_index: &TantivyWikiIndex, e0: &str, e1: &str) -> f32 {
        if e0 == e1 {
            return 1.0;
        }
        // en_uris = tuple(sorted({e1, e2}))
        let ens_in_links = [
            self.get_in_links(&wiki_index, &[e0]),
            self.get_in_links(&wiki_index, &[e1]),
        ];
        // trace!("ens_in_links({}, {}): {:?}", e0, e1, ens_in_links);
        let (min, max) = if ens_in_links[0] > ens_in_links[1] {
            (ens_in_links[1] as f32, ens_in_links[0] as f32)
        } else {
            (ens_in_links[0] as f32, ens_in_links[1] as f32)
        };

        if min == 0.0 {
            return 0.0;
        }
        let conj = self.get_in_links(&wiki_index, &[e0, e1]) as f32;
        if conj == 0.0 {
            return 0.0;
        }
        let numerator = max.ln() - conj.ln();
        let denominator = (wiki_index.len() as f32).ln() - min.ln();
        let rel = 1.0 - (numerator / denominator);
        if rel < 0.0 {
            return 0.0;
        }
        rel
    }

    /// Returns "and" occurrences of entities in the corpus.
    fn get_in_links(&mut self, wiki_index: &TantivyWikiIndex, en_uris: &[&str]) -> usize {
        use std::collections::HashSet;
        let uri_hash = hash_str_slice(&en_uris);
        if let Some(values) = self.in_links.get(&uri_hash) {
            return *values;
        }
        let en_uris = en_uris
            .iter()
            // FIXME: for some reason pages were loaded in lowercase?
            .map(|v| v.replace(" ", "_").to_lowercase())
            .collect::<HashSet<String>>()
            .into_iter()
            .collect::<Vec<_>>();

        // trace!("get_in_links(..., {:?}) :: {}", en_uris, uri_hash);
        let values = wiki_index.count_mutual_outlinks(&en_uris);
        // trace!("values: {}", values);
        self.in_links.insert(uri_hash, values);
        values
    }

    /// coherence_score = sum_e_i(rel(e_i, en)) / len(ens) - 1
    fn get_coherence_score(
        &mut self,
        wiki_index: &TantivyWikiIndex,
        mention: &str,
        entity: &str,
        disambiguated_entities: &HashMap<String, String>,
    ) -> f32 {
        let coherence_score: f32 = disambiguated_entities
            .iter()
            .filter(|(mention_i, _)| mention_i != &mention)
            .map(|(_, entity_i)| self.get_mw_rel(&wiki_index, entity_i, entity))
            .sum();
        let divisor = if disambiguated_entities.len() - 1 != 0 {
            (disambiguated_entities.len() - 1) as f32
        } else {
            0.0
        };
        coherence_score / divisor
    }

    /// Return the top K percent of entities based on relevance score.
    fn get_top_k(&self, mention: &str) -> Vec<&str> {
        let mention_scores = &self.rel_scores[mention];
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
            if rel_score != prev_rel_score {
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