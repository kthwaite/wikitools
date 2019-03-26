pub mod extract;
pub mod template;
pub mod writer;

pub use self::{
    extract::extract_templates,
    template::Template,
    writer::{FileTemplateWriter, TemplateWriter},
};
use pbr::ProgressBar;
use rayon::prelude::*;

use std::fs::File;
use std::path::Path;
use std::sync::Mutex;


use core::indices::WikiDumpIndices;
use core::multistream::open_seek_bzip;

/// Fetch templates from a Wikipedia dump, writing them to file.
///
/// After extracting the indices of template pages from an index file, pass the
/// indices to this function along with the path to a Wikipedia dump
/// multistream, and template pages will be written to an uncompressed
/// psuedo-XML file.
///
/// # Arguments
///
/// * `indices` - WikiDumpIndices indicating the offsets within the data file
///     for bundles containing template pages.
/// * `data` - Path to the Wikipedia dump multistream bz2.
/// * `output_path` - Output path to write the templates file to.
///
pub fn compile_templates(indices: &WikiDumpIndices, data: &Path, output_path: &Path) {
    let mut idx = indices.keys().cloned().collect::<Vec<_>>();
    idx.sort();
    let pbar = Mutex::new(ProgressBar::new(idx.len() as u64));
    let out_file = File::create(output_path).unwrap();
    let ftw = FileTemplateWriter::new(out_file);

    idx.into_par_iter().for_each(|index| {
        let dx = open_seek_bzip(&data, index).unwrap();
        extract_templates(dx, &ftw);
        {
            let mut prog = pbar.lock().unwrap();
            prog.inc();
        }
    });
}

#[cfg(test)]
mod test {
    use super::*;
    use std::cell::RefCell;
    use std::io::{self, Cursor};

    #[derive(Clone, Debug, Default)]
    struct TestTemplateWriter {
        pub templates: RefCell<Vec<Template>>,
    }

    impl TemplateWriter for TestTemplateWriter {
        fn write_template_impl(&self, template: Template) -> io::Result<()> {
            self.templates.borrow_mut().push(template);
            Ok(())
        }
    }

    #[test]
    fn test_extraction() {
        let test_xml = r#"
        <page>
            <title>Not a Test Template</title>
            <text>Invalid text</text>
        </page>
        <page>
            <title>Template:Test Template</title>
            <text>Valid text</text>
        </page>
        <page>
            <title>Template Test</title>
            <text>Invalid text</text>
        </page>
        <page>
            <title>Template:Second Template</title>
            <text>Another set of text</text>
        </page>
        "#;
        let reader = Cursor::new(test_xml);
        let tw = TestTemplateWriter::default();
        extract_templates(reader, &tw);
        let templates = tw.templates.into_inner();
        assert_eq!(templates.len(), 2);
        let template = &templates[0];
        assert_eq!(template.title(), "Template:Test Template");
        assert_eq!(template.page(), "Valid text");
        let template = &templates[1];
        assert_eq!(template.title(), "Template:Second Template");
        assert_eq!(template.page(), "Another set of text");
    }
}
