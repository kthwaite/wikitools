use serde::{Deserialize, Serialize};

/// Wikipedia category label.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Category(pub String);

impl Category {
    /// Get the fully qualified name of a category.
    pub fn fqn(&self) -> String {
        format!("Category:{}", self.0)
    }
}
