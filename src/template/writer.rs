use std::fs::File;
use std::io::{self, Stdout, BufWriter, Write};
use std::sync::Mutex;

use crate::template::Template;


pub trait TemplateWriter {
    fn write_template(&self, title: String, page: String) -> io::Result<()> {
        let template : Template = Template::from_unclean(title, page);
        self.write_template_impl(template)?;
        Ok(())
    }
    fn write_template_impl(&self, template: Template) -> io::Result<()>;
}


/// Thread-safe Template writer for writing to (uncompressed) file.
pub struct FileTemplateWriter {
    writer: Mutex<io::BufWriter<File>>
}

static FTW_CAP : usize = 8192 * 1024;

impl FileTemplateWriter {
    /// Create a new FileTemplateWriter from a File handle.
    pub fn new(file: File) -> Self {
        let buf = BufWriter::with_capacity(FTW_CAP, file);
        FileTemplateWriter {
            writer: Mutex::new(buf)
        }
    }
}

impl TemplateWriter for FileTemplateWriter {
    /// Write a Template to the wrapped File.
    fn write_template_impl(&self, template: Template) -> io::Result<()> {
        let mut output = self.writer.lock().unwrap();
        writeln!(&mut output, "{}", template)?;
        Ok(())
    }
}


/// Thread-safe Template writer for writing to stdout.
pub struct StdoutTemplateWriter(Stdout);

impl StdoutTemplateWriter {
    pub fn new() -> Self {
        StdoutTemplateWriter(io::stdout())
    }
}

impl TemplateWriter for StdoutTemplateWriter {
    /// Write template to stdout.
    fn write_template_impl(&self, template: Template) -> io::Result<()> {
        let mut output = self.0.lock();
        write!(&mut output, "{}", template)?;
        Ok(())
    }
}
