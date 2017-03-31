use querier_x_writer::QuerierToWriter;
use rusqlite::{self, Transaction};
use std::sync::mpsc::{Receiver, TryRecvError};
use std::thread::{self, JoinHandle};
use utils::open_database;

pub fn start_writer(receiver: Receiver<QuerierToWriter>) -> JoinHandle<()> {
    thread::spawn(move || {
        let receiver = receiver;

        let mut conn = open_database();

        let mut exit = false;

        while !exit {
            match receiver.try_recv() {
                Ok(packet_enum) => {
                    match packet_enum {
                        QuerierToWriter::Exit => {
                            exit = true;
                        }
                        QuerierToWriter::Packet(pages, links) => {
                            let transaction = conn.transaction().unwrap();

                            for page in pages {
                                add_page_to_database(&transaction, page.0, page.1);
                            }
                            for link in links {
                                add_link_to_database(&transaction, link.0, link.1);
                            }

                            transaction.commit().unwrap();
                        }
                    }
                }
                Err(TryRecvError::Disconnected) => {}
                Err(TryRecvError::Empty) => {}
            }
        }
    })
}

fn add_link_to_database(trans: &Transaction, to_ext_page_id: String, from_ext_page_id: String) {
    let to_page_id_opt = get_page_id(trans, to_ext_page_id);
    let from_page_id_opt = get_page_id(trans, from_ext_page_id);

    if let Some(to_page_id) = to_page_id_opt {
        if let Some(from_page_id) = from_page_id_opt {
            if !has_link(trans, to_page_id, from_page_id) {
                trans.execute("INSERT INTO link (to_page_id, from_page_id, length) VALUES (?1, ?2, 1)", &[&to_page_id, &from_page_id])
                    .unwrap();
            }
        }
    }
}

fn has_link(trans: &Transaction, to_page_id: i64, from_page_id: i64) -> bool {
    let mut stmt = trans.prepare("SELECT link_id FROM link WHERE to_page_id = ?1 and from_page_id = ?2")
        .unwrap();

    let link_id_iter = stmt.query_map(&[&to_page_id, &from_page_id], |row| row.get(0)).unwrap();

    for link_id_result in link_id_iter {
        let _: i64 = link_id_result.unwrap();
        return true;
    }
    false
}

fn get_page_id(trans: &Transaction, ext_page_id: String) -> Option<i64> {
    let mut stmt = trans.prepare("SELECT page_id FROM page WHERE ext_page_id = ?1").unwrap();

    let page_id_iter = stmt.query_map(&[&ext_page_id.to_string()], |row| row.get(0)).unwrap();

    for page_id_result in page_id_iter {
        return Some(page_id_result.unwrap());
    }
    None
}

fn add_page_to_database(trans: &Transaction, title_opt: Option<String>, ext_page_id: String) {
    if let Some(title) = title_opt {
        add_page_to_database_with_title(trans, title, ext_page_id);
    } else {
        add_page_to_database_without_title(trans, ext_page_id);
    }
}

fn add_page_to_database_without_title(trans: &Transaction, ext_page_id: String) {
    let mut stmt = trans.prepare("SELECT ext_page_id FROM page WHERE ext_page_id = ?1")
        .unwrap();

    let mut ext_page_id_iter = stmt.query_map(&[&ext_page_id.to_string()], |row| row.get(0))
        .unwrap();

    let ext_page_id_opt: Option<rusqlite::Result<String>> = ext_page_id_iter.next();

    match ext_page_id_opt {
        Some(Ok(_)) => (),
        Some(err) => {
            err.unwrap();
        }
        None => {
            trans.execute("INSERT INTO page (ext_page_id) VALUES (?1)", &[&ext_page_id.to_string()])
                .unwrap();
        }
    }
}

fn add_page_title_to_database(trans: &Transaction, title: String, ext_page_id: String) {
    trans.execute("UPDATE page SET title = ?1 WHERE ext_page_id = ?2", &[&title.to_string(), &ext_page_id.to_string()])
        .unwrap();
}

fn add_page_to_database_with_title(trans: &Transaction, title: String, ext_page_id: String) {
    let mut stmt = trans.prepare("SELECT ext_page_id FROM page WHERE ext_page_id = ?1 or title = ?2")
        .unwrap();

    let mut ext_page_id_iter = stmt.query_map(&[&ext_page_id.to_string(), &title.to_string()], |row| row.get(0))
        .unwrap();

    let ext_page_id_opt: Option<rusqlite::Result<String>> = ext_page_id_iter.next();

    match ext_page_id_opt {
        Some(Ok(_)) => {
            add_page_title_to_database(trans, title, ext_page_id);
        }
        Some(err) => {
            err.unwrap();
        }
        None => {
            trans.execute("INSERT INTO page (title, ext_page_id) VALUES (?1, ?2)", &[&title.to_string(), &ext_page_id.to_string()])
                .unwrap();
        }
    }
}
