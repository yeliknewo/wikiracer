extern crate hyper;
extern crate hyper_native_tls;
extern crate rusqlite;
extern crate json;

use std::sync::mpsc::channel;

mod pageid_x_querier;
mod querier_x_writer;
mod writer_x_pageid;

mod pageid;
mod querier;
mod writer;

mod utils;

use pageid::start_pageid;
use pageid_x_querier::PageIdToQuerier;
use querier::start_querier;
use querier_x_writer::QuerierToWriter;
// use utils::create_database;
use writer::start_writer;
use writer_x_pageid::WriterToPageId;

fn main() {
    let (pageid_sender, querier_receiver) = channel::<PageIdToQuerier>();
    let (querier_sender, writer_receiver) = channel::<QuerierToWriter>();
    let (writer_sender, pageid_receiver) = channel::<WriterToPageId>();

    // let conn = create_database();
    // conn.close().unwrap();
    //
    // pageid_sender.send(PageIdToQuerier::Packet("1095706".to_string(), None)).unwrap();

    let pageid = start_pageid(10, pageid_receiver, pageid_sender);
    let querier = start_querier(10, querier_receiver, querier_sender);
    let writer = start_writer(10, writer_receiver, writer_sender);

    pageid.join().unwrap();
    querier.join().unwrap();
    writer.join().unwrap();
}
