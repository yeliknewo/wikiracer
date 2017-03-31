extern crate hyper;
extern crate hyper_native_tls;
extern crate rusqlite;
extern crate json;
extern crate time;

use std::io::{self, Read};
use std::sync::mpsc::channel;

mod main_x_page_id;
mod page_id_x_querier;
mod querier_x_writer;

mod page_id;
mod querier;
mod writer;

mod utils;

use main_x_page_id::MainToPageId;
use page_id::start_pageid;
use page_id_x_querier::PageIdToQuerier;
use querier::start_querier;
use querier_x_writer::QuerierToWriter;

use utils::create_database;
use writer::start_writer;

fn main() {
    let start_time = time::now();

    let (page_id_sender, querier_receiver) = channel::<PageIdToQuerier>();
    let (querier_sender, writer_receiver) = channel::<QuerierToWriter>();
    let (main_sender, page_id_receiver) = channel::<MainToPageId>();

    let conn = create_database();
    conn.close().unwrap();

    main_sender.send(MainToPageId::Packet("1095706".to_string())).unwrap();

    let pageid = start_pageid(10, page_id_sender, page_id_receiver);
    let querier = start_querier(10, querier_receiver, querier_sender);
    let writer = start_writer(writer_receiver);

    let mut exit = false;

    let mut buffer = String::new();
    let stdin = io::stdin();

    while !exit {
        {
            let mut handle = stdin.lock();
            handle.read_to_string(&mut buffer).unwrap();
        }
        match buffer.as_str() {
            "exit" => {
                println!("Exiting");
                exit = true;
            }
            _ => (),
        }
    }

    main_sender.send(MainToPageId::Exit).unwrap();

    pageid.join().unwrap();
    querier.join().unwrap();
    writer.join().unwrap();

    let end_time = time::now();

    println!("Start Time: {}", start_time.ctime());
    println!("End Time: {}", end_time.ctime());
    println!("Run Time: {}", end_time - start_time);
}
