#[derive(Debug)]
pub enum WriterToPageId {
    Packet(String, String),
    Exit,
}
