use std::io::BufRead;

use quick_xml as qx;

use super::writer::TemplateWriter;

/// Extract templates from a stream and pass them to a TemplateWriter.
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
            Ok(Event::Start(ref tag)) => match tag.name() {
                b"page" => in_page = true,
                b"title" => {
                    if in_page {
                        title = reader.read_text(b"title", &mut text_buf).unwrap();
                        if title.starts_with("Template:") {
                            in_template = true;
                        }
                    }
                }
                b"text" => {
                    if in_template {
                        page = reader.read_text(b"text", &mut text_buf).unwrap();
                    }
                }
                _ => (),
            },
            Ok(Event::End(ref tag)) => {
                if let b"page" = tag.name() {
                    in_page = false;
                    if in_template {
                        writer.write_template(title, page).unwrap();
                        title = String::new();
                        page = String::new();
                    }
                    in_template = false;
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => (),
            Err(_) => break,
        }
        buf.clear();
    }
}
