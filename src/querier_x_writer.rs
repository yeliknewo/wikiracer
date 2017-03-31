use json::JsonValue;

#[derive(Debug)]
pub enum QuerierToWriter {
    Packet(String, JsonValue),
    Exit,
}
