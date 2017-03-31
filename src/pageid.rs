use pageid_x_querier::PageIdToQuerier;
use rusqlite::Connection;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::thread::{self, JoinHandle};
use utils::open_database;
use writer_x_pageid::WriterToPageId;

pub fn start_pageid(pageids_buffer_size: usize, pageid_receiver: Receiver<WriterToPageId>, pageid_sender: Sender<PageIdToQuerier>) -> JoinHandle<()> {
    thread::spawn(move || {
        let pageid_receiver = pageid_receiver;
        let pageid_sender = pageid_sender;

        let conn = open_database();

        let mut exit = false;

        let mut pageids: Vec<(String, Option<String>)> = vec![];

        while !exit {
            // println!("Pageid Buffer Length: {}", pageids.len());
            if pageids.len() < pageids_buffer_size {
                match pageid_receiver.try_recv() {
                    Ok(packet_enum) => {
                        match packet_enum {
                            WriterToPageId::Exit => {
                                exit = true;
                            }
                            WriterToPageId::Packet(pageid, lhcontinue) => {
                                pageids.push((pageid, Some(lhcontinue)));
                            }
                        }
                    }
                    Err(TryRecvError::Disconnected) => {}
                    Err(TryRecvError::Empty) => {
                        if let Some(target) = get_next_page_id_target(&conn) {
                            pageids.push((target, None));
                        }
                    }
                }
            } else {
                for packet in pageids.drain(..) {
                    pageid_sender.send(PageIdToQuerier::Packet(packet.0, packet.1)).unwrap();
                }
            }
        }

        for packet in pageids.drain(..) {
            pageid_sender.send(PageIdToQuerier::Packet(packet.0, packet.1)).unwrap();
        }

        pageid_sender.send(PageIdToQuerier::Exit).unwrap();
    })
}

fn get_next_page_id_target(conn: &Connection) -> Option<String> {
    let mut stmt = conn.prepare("SELECT ext_page_id FROM page WHERE title IS NULL").unwrap();

    let ext_page_id_iter = stmt.query_map(&[], |row| row.get(0)).unwrap();

    for ext_page_id_result in ext_page_id_iter {
        return Some(ext_page_id_result.unwrap());
    }
    None
}
