use main_x_page_id::MainToPageId;
use page_id_x_querier::PageIdToQuerier;
use rusqlite::Connection;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::thread::{self, JoinHandle};
use utils::open_database;

pub fn start_pageid(buffer_size: usize, sender: Sender<PageIdToQuerier>, receiver: Receiver<MainToPageId>) -> JoinHandle<()> {
    thread::spawn(move || {
        let receiver = receiver;
        let sender = sender;

        let conn = open_database();

        let mut exit = false;

        let mut buffer: Vec<PageIdToQuerier> = vec![];

        let mut last_new_page_id_opt = None;

        while !exit {
            // println!("Pageid Buffer Length: {}", pageids.len());
            if buffer.len() < buffer_size {
                match receiver.try_recv() {
                    Ok(packet_enum) => {
                        match packet_enum {
                            MainToPageId::Exit => {
                                exit = true;
                            }
                            MainToPageId::Packet(page_id) => {
                                buffer.push(PageIdToQuerier::Packet(page_id));
                            }
                        }
                    }
                    Err(TryRecvError::Disconnected) => {}
                    Err(TryRecvError::Empty) => {
                        if let Some(target) = get_next_page_id_target(&conn) {
                            if last_new_page_id_opt.is_some() {
                                let last_new_page_id = last_new_page_id_opt.take().unwrap();
                                if last_new_page_id != target {
                                    last_new_page_id_opt = Some(target.clone());
                                    buffer.push(PageIdToQuerier::Packet(target));
                                } else {
                                    last_new_page_id_opt = Some(last_new_page_id);
                                }
                            } else {
                                last_new_page_id_opt = Some(target.clone());
                                buffer.push(PageIdToQuerier::Packet(target));
                            }
                        }

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

        sender.send(PageIdToQuerier::Exit).unwrap();
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
