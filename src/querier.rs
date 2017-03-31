use hyper::client::Client;
use hyper::net::HttpsConnector;
use hyper_native_tls::NativeTlsClient;
use json::{self, JsonValue};
use pageid_x_querier::PageIdToQuerier;
use querier_x_writer::QuerierToWriter;
use std::io::Read;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::thread::{self, JoinHandle};

pub fn start_querier(query_buffer_size: usize, querier_receiver: Receiver<PageIdToQuerier>, querier_sender: Sender<QuerierToWriter>) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut queries = vec![];
        let querier_receiver = querier_receiver;
        let querier_sender = querier_sender;

        let ssl = NativeTlsClient::new().unwrap();
        let connector = HttpsConnector::new(ssl);
        let client = Client::with_connector(connector);

        let mut exit = false;
        while !exit {
            // println!("Querier Buffer Length: {}", queries.len());
            if queries.len() < query_buffer_size {
                match querier_receiver.try_recv() {
                    Ok(packet_enum) => {
                        match packet_enum {
                            PageIdToQuerier::Exit => {
                                exit = true;
                            }
                            PageIdToQuerier::Packet(pageid, lhcontinue_opt) => {
                                queries.push((pageid.clone(), query_wikipedia(&client, pageid.clone(), lhcontinue_opt)));
                            }
                        }
                    }
                    Err(TryRecvError::Disconnected) => {}
                    Err(TryRecvError::Empty) => {}
                }
            } else {
                for packet in queries.drain(..) {
                    querier_sender.send(QuerierToWriter::Packet(packet.0, packet.1)).unwrap();
                }
            }
        }

        for packet in queries.drain(..) {
            querier_sender.send(QuerierToWriter::Packet(packet.0, packet.1)).unwrap();
        }

        querier_sender.send(QuerierToWriter::Exit).unwrap();
    })
}

fn query_wikipedia(client: &Client, pageids: String, continue_opt: Option<String>) -> JsonValue {
    let url = {
        if let Some(continue_string) = continue_opt {
            format!("https://en.wikipedia.org/w/api.php?action=query&format=json&pageids={}&prop=linkshere&lhprop=pageid&lhlimit=500&lhnamespace=0&redirects{}", pageids, continue_string)
        } else {
            format!("https://en.wikipedia.org/w/api.php?action=query&format=json&pageids={}&prop=linkshere&lhprop=pageid&lhlimt=500&lhnamespace=0", pageids)
        }
    };
    // println!("{:?}", url);
    let mut resp = client.get(url.as_str())
        .send()
        .unwrap();
    let mut body = vec![];
    resp.read_to_end(&mut body).unwrap();
    let string = String::from_utf8_lossy(&body);
    // println!("{:?}", string);
    json::parse(&string).unwrap()
}
