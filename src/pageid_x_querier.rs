#[derive(Debug)]
pub enum PageIdToQuerier {
    Packet(String, Option<String>),
    Exit,
}
