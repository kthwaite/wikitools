use std::fmt::{self};
use std::fs::File;
use std::io::{self, BufRead, Stdout, BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;

use pbr::ProgressBar;
use quick_xml as qx;
use rayon::prelude::*;

use utils::open_seek_bzip;
use indices::WikiDumpIndices;


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



pub trait TemplateWriter {
    fn write_template(&self, title: String, page: String) {
        let template : Template = Template::from_unclean(title, page);
        self.write_template_impl(template);
    }
    fn write_template_impl(&self, template: Template);
}

pub struct FileTemplateWriter {
    writer: Mutex<io::BufWriter<File>>
}

static FTW_CAP : usize = 8192 * 1024;

impl FileTemplateWriter {
    pub fn new(file: File) -> Self {
        let buf = BufWriter::with_capacity(FTW_CAP, file);
        FileTemplateWriter {
            writer: Mutex::new(buf)
        }
    }
}

impl TemplateWriter for FileTemplateWriter {
    fn write_template_impl(&self, template: Template) {
        let mut output = self.writer.lock().unwrap();
        writeln!(&mut output, "{}", template);
    }
}


pub struct StdoutTemplateWriter(Stdout);

impl StdoutTemplateWriter {
    pub fn new() -> Self {
        StdoutTemplateWriter(io::stdout())
    }
}

impl TemplateWriter for StdoutTemplateWriter {
    fn write_template_impl(&self, template: Template) {
        let mut output = self.0.lock();
        write!(&mut output, "{}", template).unwrap();
    }
}


pub fn extract_templates<R: BufRead>(stream: R, writer: &TemplateWriter) {
    use self::qx::events::Event;

    let mut reader = qx::Reader::from_reader(stream);

    let (mut buf, mut text_buf) = (Vec::new(), Vec::new());
    let mut page = String::new();
    let mut title = String::new();

    let mut in_page = false;
    let mut in_template = false;

    loop {
        match reader.read_event(&mut buf) {
            Ok(Event::Start(ref tag)) => {
                match tag.name() {
                   b"page" => in_page = true,
                   b"title" => {
                       if in_page {
                            title = reader.read_text(b"title", &mut text_buf).unwrap();
                            if title.starts_with("Template:") {
                                in_template = true;
                            }
                       }
                   },
                   b"text" => {
                        if in_template {
                            page = reader.read_text(b"text", &mut text_buf).unwrap();
                        }
                    }
                   _ => ()
                }
            },
            Ok(Event::End(ref tag)) => {
                match tag.name() {
                   b"page" => {
                       in_page = false;
                       if in_template {
                           writer.write_template(title, page);
                           title = String::new();
                           page = String::new();
                       }
                       in_template = false;
                   },
                   _ => (),
                }
            },
            Ok(Event::Eof) => break,
            Ok(_) => (),
            Err(_) => break,
        }
        buf.clear();
    }
}


pub fn compile_templates(indices: &WikiDumpIndices, data: &Path, output_path: &Path) {
    let mut idx = indices.keys().cloned().collect::<Vec<_>>();
    idx.sort();
    let pbar = Mutex::new(ProgressBar::new(idx.len() as u64));
    let out_file = File::create(output_path).unwrap();
    let ftw = FileTemplateWriter::new(out_file);

    idx.into_par_iter()
        .for_each(|index| {
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
    use std::io::Cursor;

    #[derive(Clone, Debug, Default)]
    struct TestTemplateWriter {
        pub templates: RefCell<Vec<Template>>
    }

    impl TemplateWriter for TestTemplateWriter {
        fn write_template_impl(&self, template: Template) {
            self.templates.borrow_mut().push(template);
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
        assert_eq!(template.title, "Template:Test Template");
        assert_eq!(template.page, "Valid text");
        let template = &templates[1];
        assert_eq!(template.title, "Template:Second Template");
        assert_eq!(template.page, "Another set of text");
    }
}
