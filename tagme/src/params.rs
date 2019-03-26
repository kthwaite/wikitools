/// Parameters for a TagMe worker.
#[derive(Clone, Copy, Debug)]
pub struct TagMeParams {
    /// Minimum link probability for candidate surface forms.
    pub link_probability_threshold: f32,
    /// Minimum link probability for candidate entities.
    pub candidate_mention_threshold: f32,
    /// Top K percent of entities to retain, based on relevance score, during
    /// filtering.
    pub k_th: f32, // TODO: rename
    /// Minimum n-gram size for surface forms.
    pub ngram_min: usize,
    /// Maximum n-gram size for surface forms.
    pub ngram_max: usize,
}

impl TagMeParams {
    pub fn with_link_probability_threshold(self, link_probability_threshold: f32) -> Self {
        TagMeParams {
            link_probability_threshold,
            ..self
        }
    }
    pub fn with_candidate_mention_threshold(self, candidate_mention_threshold: f32) -> Self {
        TagMeParams {
            candidate_mention_threshold,
            ..self
        }
    }
    pub fn with_k_th(self, k_th: f32) -> Self {
        TagMeParams { k_th, ..self }
    }
    pub fn with_ngram_min(self, ngram_min: usize) -> Self {
        TagMeParams { ngram_min, ..self }
    }
    pub fn with_ngram_max(self, ngram_max: usize) -> Self {
        TagMeParams { ngram_max, ..self }
    }
}

impl Default for TagMeParams {
    fn default() -> Self {
        TagMeParams {
            link_probability_threshold: 0.001,
            candidate_mention_threshold: 0.02,
            k_th: 0.3,
            ngram_max: 6,
            ngram_min: 2,
        }
    }
}
