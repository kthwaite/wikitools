use path::Path;
use std::io::BufRead;

use regex::Regex;

#[derive(Debug, Default, Clone)]
pub struct SiteInfo {
    pub base: String,
    pub url_base: String,
    pub template_prefix: String,
    pub template_namespace: String,
    pub known_namespaces: Vec<String>,
}

/// Determine site info for a Wikipedia dump.
pub fn collect_siteinfo<R: BufRead>(data: &R) -> SiteInfo {
    let tag_rx = Regex::new(r#"(.*?)<(/?\w+)[^>]*>(?:([^<]*)(<.*?>)?)?"#).unwrap();
    let mut site_info: SiteInfo = Default::default();
    let mut known_namespaces = vec![];
    for line in data.lines().map(|line| line.unwrap()) {
        if let Some(mch) = tag_rx.captures(&line) {
            let tag = mch.get(2).unwrap().as_str();
            match tag {
                "base" => {
                    site_info.base = mch.get(3).unwrap().as_str().to_owned();
                    site_info.url_base = base[0..base.rfind("/").unwrap()].to_owned();
                }
                "namespace" => {
                    let ns = mch.get(3).unwrap().as_str().to_owned();
                    if !ns.is_empty() {
                        site_info.known_namespaces.push(ns);
                    }
                    if line.find(r#"key="10""#).is_some() {
                        site_info.template_namespace = mch.get(3).unwrap().as_str().to_owned();
                        site_info.template_prefix = format!("{}:", template_namespace);
                    }
                }
                "/siteinfo" => break,
            }
        }
    }
    site_info
}

#[cfg(test)]
mod test {
    #[test]
    fn test_siteinfo() {}
}
