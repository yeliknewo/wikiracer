use hyper::client::Client;
use hyper::net::HttpsConnector;
use hyper_native_tls::NativeTlsClient;
use json::{self, JsonValue};
use page_id_x_querier::PageIdToQuerier;
use querier_x_writer::QuerierToWriter;
use std::io::Read;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::thread::{self, JoinHandle};
use utils::WikiRacerError;

pub fn start_querier(buffer_size: usize, receiver: Receiver<PageIdToQuerier>, sender: Sender<QuerierToWriter>) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut buffer: Vec<QuerierToWriter> = vec![];
        let receiver = receiver;
        let sender = sender;

        let ssl = NativeTlsClient::new().unwrap();
        let connector = HttpsConnector::new(ssl);
        let client = Client::with_connector(connector);

        let mut exit = false;
        while !exit {
            if buffer.len() < buffer_size {
                match receiver.try_recv() {
                    Ok(packet_enum) => {
                        match packet_enum {
                            PageIdToQuerier::Exit => {
                                exit = true;
                            }
                            PageIdToQuerier::Packet(page_id) => {
                                let mut base_json = query_wikipedia(&client, page_id.clone(), None);
                                let mut pages = vec![];
                                let mut links = vec![];
                                while let Some(hl_continue) = add_to_database(&mut pages, &mut links, &base_json).unwrap() {
                                    base_json = query_wikipedia(&client, page_id.clone(), Some(hl_continue));
                                }
                                let packet = QuerierToWriter::Packet(pages, links);
                                buffer.push(packet);
                            }
                        }
                    }
                    Err(TryRecvError::Disconnected) => {}
                    Err(TryRecvError::Empty) => {
                        for packet in buffer.drain(..) {
                            sender.send(packet).unwrap();
                        }
                    }
                }
            } else {
                for packet in buffer.drain(..) {
                    sender.send(packet).unwrap();
                }
            }
        }

        for packet in buffer.drain(..) {
            sender.send(packet).unwrap();
        }

        sender.send(QuerierToWriter::Exit).unwrap();
    })
}

fn query_wikipedia(client: &Client, pageids: String, lh_continue_opt: Option<String>) -> JsonValue {
    let url = {
        if let Some(continue_string) = lh_continue_opt {
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

fn add_to_database(out_pages: &mut Vec<(Option<String>, String)>, out_links: &mut Vec<(String, String)>, base: &JsonValue) -> Result<Option<String>, WikiRacerError> {
    let mut continue_opt = None;

    match base {
        &JsonValue::Object(ref base_obj) => {
            if let Some(json_continue) = base_obj.get("continue") {
                match json_continue {
                    &JsonValue::Object(ref json_continue_obj) => {
                        let mut lhcontinue_opt = None;

                        if let Some(lhcontinue) = json_continue_obj.get("lhcontinue") {
                            match lhcontinue {
                                &JsonValue::String(ref lhcontinue_string) => {
                                    lhcontinue_opt = Some(lhcontinue_string.to_string());
                                }
                                &JsonValue::Short(ref lhcontinue_short) => {
                                    lhcontinue_opt = Some(lhcontinue_short.to_string());
                                }
                                _ => (),
                            }
                        }

                        if let Some(lhcontinue) = lhcontinue_opt {
                            continue_opt = Some(format!("&lhcontinue={}&continue=||", lhcontinue));
                        }
                    }
                    _ => (),
                }
            }
            // println!("{:?}", base_obj);
            if let Some(query) = base_obj.get("query") {
                // println!("{:?}", query);
                match query {
                    &JsonValue::Object(ref query_obj) => {
                        // println!("{:?}", query_obj);
                        if let Some(pages) = query_obj.get("pages") {
                            // println!("{:?}", pages);
                            match pages {
                                &JsonValue::Object(ref pages_obj) => {
                                    // println!("{:?}", pages_obj);
                                    for page_entry in pages_obj.iter() {
                                        let page_id = page_entry.0.to_string();
                                        let page = page_entry.1;
                                        // println!("{:?} --- {:?}", page_id, page);
                                        match page {
                                            &JsonValue::Object(ref page_obj) => {
                                                // println!("{:?}", page_obj);
                                                if let Some(page_title) = page_obj.get("title") {
                                                    // println!("{:?}", page_title);
                                                    match page_title {
                                                        &JsonValue::String(ref page_title_string) => {
                                                            // if continue_opt.is_none() {
                                                            println!("{:?}", page_title_string);
                                                            // }
                                                            let title_str = page_title_string.to_string();
                                                            out_pages.push((Some(title_str), page_id.clone()));
                                                        }
                                                        &JsonValue::Short(ref page_title_short) => {
                                                            // if continue_opt.is_none() {
                                                            println!("{:?}", page_title_short);
                                                            // }
                                                            let title_str = page_title_short.to_string();
                                                            out_pages.push((Some(title_str), page_id.clone()));
                                                        }
                                                        _ => return Err(WikiRacerError::FormatError("Page Title was not a string or short")),
                                                    }
                                                }
                                                if let Some(page_links_here) = page_obj.get("linkshere") {
                                                    // println!("{:?}", page_links_here);
                                                    match page_links_here {
                                                        &JsonValue::Array(ref page_links_here_vec) => {
                                                            // println!("{:?}", page_links_here_vec);
                                                            for page_link_here in page_links_here_vec {
                                                                // println!("{:?}", page_link_here);
                                                                match page_link_here {
                                                                    &JsonValue::Object(ref page_link_here_obj) => {
                                                                        // println!("{:?}", page_link_here_obj);
                                                                        if let Some(page_link_here_id) = page_link_here_obj.get("pageid") {
                                                                            // println!("{:?}", page_link_here_id);
                                                                            match page_link_here_id {
                                                                                &JsonValue::Number(ref page_link_here_id_num) => {
                                                                                    // println!("{:?}", page_link_here_id_num);
                                                                                    if let Some(page_link_here_id_num_u64) = page_link_here_id_num.as_fixed_point_u64(0) {
                                                                                        // println!("{:?}", page_link_here_id_num_u64);
                                                                                        let other_page_id_string = page_link_here_id_num_u64.to_string();
                                                                                        let other_page_id_str = other_page_id_string.to_string();
                                                                                        out_pages.push((None, other_page_id_str.clone()));
                                                                                        out_links.push((page_id.clone(), other_page_id_str.clone()));
                                                                                    }
                                                                                }
                                                                                _ => return Err(WikiRacerError::FormatError("Page Link Here Id was not a number")),
                                                                            }
                                                                        }
                                                                    }
                                                                    _ => return Err(WikiRacerError::FormatError("Page Link Here was not an object")),
                                                                }
                                                            }
                                                        }
                                                        _ => return Err(WikiRacerError::FormatError("Page Links Here was not an array")),
                                                    }
                                                }
                                            }
                                            _ => return Err(WikiRacerError::FormatError("Page was not an object")),
                                        }
                                    }
                                }
                                _ => return Err(WikiRacerError::FormatError("Pages was not an object")),
                            }
                        }
                    }
                    _ => return Err(WikiRacerError::FormatError("Query was not an object")),
                }
            }
        }
        _ => return Err(WikiRacerError::FormatError("Base was not an object")),
    }
    Ok(continue_opt)
}
