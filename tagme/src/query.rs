#![allow(dead_code, unused_imports)]
use crate::query_text::Query;
use crate::stopwords::STOPWORDS_EN;
use crate::tag_me::TagMe;
use itertools::Itertools;
use log::{debug, error, info, trace};
use rayon::prelude::*;
use std::collections::{hash_map::DefaultHasher, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use storage::{
    fst::WikiAnchors,
    surface_form::{SurfaceForm, SurfaceFormStoreRead},
    tantivy::TantivyWikiIndex,
};

/// Hash an arbitrary slice of str.
fn hash_str_slice<S: AsRef<str> + Hash>(en_uris: &[S]) -> u64 {
    let mut s = DefaultHasher::new();
    for uri in en_uris {
        uri.hash(&mut s);
    }
    s.finish()
}

#[derive(Debug)]
pub struct TagMeQuery {
    query: Query,
    rho_th: f32,

    link_probabilities: HashMap<String, f32>,
    mutual_outlinks: HashMap<u64, usize>,
    rel_scores: HashMap<String, HashMap<String, f32>>,
    disamb_ens: HashMap<String, usize>,
}

impl TagMeQuery {
    pub fn new(query_str: &str, rho_th: f32) -> Self {
        TagMeQuery {
            query: Query::new(0, query_str),
            rho_th,

            link_probabilities: HashMap::default(),
            mutual_outlinks: HashMap::default(),
            rel_scores: HashMap::default(),
            disamb_ens: HashMap::default(),
        }
    }

    pub fn query_text(&self) -> &Query {
        &self.query
    }

    ///
    /// Hasibi et al.:
    ///     To detect entity mentions, TAGME matches all n-grams of the input text,
    ///     up to n = 6, against the surface form dictionary. For an n-gram contained
    ///     by another one, TAGME drops the shorter n-gram, if it has lower link
    ///     probability than the longer one. The output of this step is a set of
    ///     mentions with their corresponding candidate entities.
    pub fn parse<S: SurfaceFormStoreRead>(
        &mut self,
        tag_me: &TagMe<S>,
    ) -> HashMap<String, HashMap<String, f32>> {
        let (mut entities, link_probabilities) = tag_me.entities_for_query(&self.query);
        self.link_probabilities = link_probabilities;
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
            for mention_j in sorted_mentions.iter().skip(i + 1) {
                if mention_j.find(m_i).is_some()
                    && (self.link_probability_of(m_i) < self.link_probability_of(mention_j))
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

    /// Generate entity-entity scores for a set of candidate entities.
    fn generate_entity_pairs<S: SurfaceFormStoreRead>(
        tag_me: &TagMe<S>,
        candidate_entities: &HashMap<String, HashMap<String, f32>>,
    ) -> HashMap<u64, usize> {
        let mut all_entities = candidate_entities
            .values()
            .flat_map(|map| map.keys())
            .map(|key| key.replace(" ", "_"))
            .collect::<HashSet<String>>()
            .into_iter()
            .collect::<Vec<_>>();
        info!("Found {} entities...", all_entities.len());
        all_entities.sort_by(|s0, s1| s0.partial_cmp(s1).unwrap());
        let all_entities = all_entities
            .into_iter()
            .tuple_combinations()
            .collect::<Vec<_>>();
        info!("Found {} entity pairs...", all_entities.len());

        let wiki_index = &tag_me.wiki_index;
        all_entities
            .into_par_iter()
            .map(|(e0, e1)| {
                let mutual_outlinks = wiki_index.count_mutual_outlinks(&[e0.as_str(), e1.as_str()]);
                (hash_str_slice(&[e0, e1]), mutual_outlinks)
            })
            .collect()
    }

    fn build_initial_votes<S: SurfaceFormStoreRead>(
        &mut self,
        tag_me: &TagMe<S>,
        candidate_entities: &HashMap<String, HashMap<String, f32>>,
    ) -> HashMap<String, HashMap<String, f32>> {
        let mut rel_scores: HashMap<String, HashMap<String, f32>> = Default::default();

        for mention_i in candidate_entities.keys() {
            trace!("Voting on {}", mention_i);
            rel_scores.insert(mention_i.to_string(), HashMap::default());
            for entity_mention_i in candidate_entities.get(mention_i).unwrap().keys() {
                let vote_sum = candidate_entities.keys().fold(0.0f32, |acc, mention_j| {
                    if mention_i == mention_j {
                        return acc;
                    }
                    acc + self.get_vote(
                        &tag_me.wiki_index,
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
        rel_scores
    }

    /// Populate the list of entity scores, pruning entities below the candidate
    /// mention threshold
    fn populate_entity_scores(
        &mut self,
        candidate_mention_threshold: f32,
        candidate_entities: &HashMap<String, HashMap<String, f32>>,
        rel_scores: HashMap<String, HashMap<String, f32>>,
    ) {
        for mention_i in rel_scores.keys() {
            for entity_mention_i in rel_scores[mention_i].keys() {
                let candidate_mention = candidate_entities[mention_i][entity_mention_i];
                if candidate_mention >= candidate_mention_threshold {
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
    }

    /// Pick the best entity for each mention, returning a map of mentions to
    /// entities.
    fn pick_best_entity_for_each_mention<S: SurfaceFormStoreRead>(
        &mut self,
        tag_me: &TagMe<S>,
        candidate_entities: &HashMap<String, HashMap<String, f32>>,
    ) -> HashMap<String, String> {
        let mut disambiguated_entities: HashMap<String, String> = Default::default();
        for mention_i in self.rel_scores.keys() {
            trace!("evaluating mention {}", mention_i);
            if self.rel_scores[mention_i].is_empty() {
                trace!("skipping mention {}, score zero", mention_i);
                continue;
            }
            let top_k_entities = self.get_top_k(&tag_me, mention_i);
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

    /// Performs disambiguation, linking each mention to a single entity.
    pub fn disambiguate<S: SurfaceFormStoreRead>(
        &mut self,
        tag_me: &TagMe<S>,
        candidate_entities: &HashMap<String, HashMap<String, f32>>,
    ) -> HashMap<String, String> {
        info!("Disambiguating...");

        info!("Precaching entity pairs...");
        self.mutual_outlinks = TagMeQuery::generate_entity_pairs(tag_me, candidate_entities);
        let rel_scores = self.build_initial_votes(tag_me, candidate_entities);
        trace!("entity votes: {:?}", rel_scores);

        info!("pruning...");
        self.populate_entity_scores(
            tag_me.params.candidate_mention_threshold,
            candidate_entities,
            rel_scores,
        );
        self.pick_best_entity_for_each_mention(tag_me, candidate_entities)
    }

    /// Prune entities
    pub fn prune<S: SurfaceFormStoreRead>(
        &mut self,
        tag_me: &TagMe<S>,
        disambiguated_entities: HashMap<String, String>,
    ) -> HashMap<String, (String, f32)> {
        disambiguated_entities
            .iter()
            .filter_map(|(mention, entity)| {
                let coh_score = self.get_coherence_score(
                    &tag_me.wiki_index,
                    mention,
                    entity,
                    &disambiguated_entities,
                );
                let rho_score = (self.link_probabilities[mention] + coh_score) / 2.0;
                if rho_score >= self.rho_th {
                    return Some((mention.to_string(), (entity.to_string(), rho_score)));
                }
                None
            })
            .collect()
    }

    /// Extract and return entities from the query phrase.
    fn extract_entities<S: SurfaceFormStoreRead>(&self) -> HashMap<String, String> {
        let ents = qry.parse(&tag_me);
        let ents = qry.disambiguate(&tag_me, &ents);
        qry.prune(&tag_me, ents)
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

    /// Calculates Milne & Witten relatedness for two entities.
    /// This implementation is based on Hasibi's implementation, which in turn
    /// is based on the 'Dexter' implementation (which is similar to TAGME implementation).
    /// - Dexter implementation: https://github.com/dexter/dexter/blob/master/dexter-core/src/main/java/it/cnr/isti/hpc/dexter/relatedness/MilneRelatedness.java
    /// - TAGME: it.acubelab.tagme.preprocessing.graphs.OnTheFlyArrayMeasure
    fn get_mw_rel(&mut self, wiki_index: &TantivyWikiIndex, e0: &str, e1: &str) -> f32 {
        trace!("\t-- get_mw_rel({}, {})", e0, e1);
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
        trace!("\t-- min: {} | max: {}", min, max);

        if min == 0.0 {
            return 0.0;
        }
        let conj = self.get_in_links(&wiki_index, &[e0, e1]) as f32;
        trace!("\t-- conj: {}", conj);
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
        let mut en_uris = en_uris
            .iter()
            .map(|v| v.replace(" ", "_"))
            .collect::<HashSet<String>>()
            .into_iter()
            .collect::<Vec<_>>();
        en_uris.sort_by(|s0, s1| s0.partial_cmp(s1).unwrap());
        let uri_hash = hash_str_slice(&en_uris);
        if let Some(values) = self.mutual_outlinks.get(&uri_hash) {
            return *values;
        }

        trace!("\t\tget_in_links(..., {:?}) :: {}", en_uris, uri_hash);
        let values = wiki_index.count_mutual_outlinks(&en_uris);
        trace!("\t\t== mutual_outlinks: {}", values);
        self.mutual_outlinks.insert(uri_hash, values);
        values
    }

    /// Return the top K percent of entities based on relevance score.
    fn get_top_k<S: SurfaceFormStoreRead>(&self, tag_me: &TagMe<S>, mention: &str) -> Vec<&str> {
        let mention_scores = &self.rel_scores[mention];
        tag_me.get_top_k(mention_scores)
    }
}
