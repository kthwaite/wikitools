use std::fmt;

/// Wikipedia Template data.
#[derive(Clone, Debug, Default)]
pub struct Template {
    title: String,
    page: String,
}

impl Template {
    pub fn from_unclean(title: String, page: String) -> Self {
        let (title, page) = Template::clean(title, page);
        Template {
            title,
            page
        }
    }

    pub fn clean(title: String, page: String) -> (String, String) {
        (title, page)
    }
}

impl fmt::Display for Template {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "<page>\n   <title>{}</title>\n   <ns>10</ns>\n   <text>{}\n   </text>\n</page>",
               self.title,
               self.page)
    }
}
