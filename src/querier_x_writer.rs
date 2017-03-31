#[derive(Debug)]
pub enum QuerierToWriter {
    Packet(Vec<(Option<String>, String)>, Vec<(String, String)>),
    Exit,
}
