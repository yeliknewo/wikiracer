extern crate hyper;
extern crate hyper_native_tls;
extern crate rusqlite;
extern crate json;

use json::JsonValue;
use rusqlite::Connection;
use hyper::client::Client;
use hyper::net::HttpsConnector;
use hyper_native_tls::NativeTlsClient;
use std::io::Read;

fn create_database() -> Connection {
    let conn = open_database();

    conn.execute("CREATE TABLE page (
                    page_id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    title           TEXT,
                    ext_page_id     TEXT
                )",
                 &[])
        .unwrap();

    conn.execute("CREATE TABLE link (
                    link_id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    to_page_id      INTEGER NOT NULL,
                    from_page_id    INTEGER NOT NULL,
                    CONSTRAINT Link_FK1 FOREIGN KEY (to_page_id) REFERENCES page(page_id),
                    CONSTRAINT Link_FK2 FOREIGN KEY (from_page_id) REFERENCES page(page_id)
                )",
                 &[])
        .unwrap();

    conn
}

fn open_database() -> Connection {
    Connection::open("database.sqlite3").unwrap()
}

#[derive(Debug)]
enum WikiRacerError {
    FormatError(&'static str),
}

fn add_link_to_database(conn: &Connection, to_ext_page_id: &str, from_ext_page_id: &str) {
    conn.execute("INSERT INTO link (to_page_id, from_page_id) VALUES ((SELECT page_id FROM page \
                  WHERE ext_page_id = ?1), (SELECT page_id FROM page WHERE ext_page_id = ?2))",
                 &[&to_ext_page_id.to_string(), &from_ext_page_id.to_string()])
        .unwrap();
}

fn add_page_to_database(conn: &Connection, title_opt: Option<&str>, ext_page_id: &str) {
    if let Some(title) = title_opt {
        add_page_to_database_with_title(conn, title, ext_page_id);
    } else {
        add_page_to_database_without_title(conn, ext_page_id);
    }
}

fn add_page_to_database_without_title(conn: &Connection, ext_page_id: &str) {
    let mut stmt = conn.prepare("SELECT ext_page_id FROM page WHERE ext_page_id = ?1")
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
            conn.execute("INSERT INTO page (ext_page_id) VALUES (?1)",
                         &[&ext_page_id.to_string()])
                .unwrap();
        }
    }
}

fn add_page_to_database_with_title(conn: &Connection, title: &str, ext_page_id: &str) {
    let mut stmt = conn.prepare("SELECT ext_page_id FROM page WHERE ext_page_id = ?1 or title = ?2")
        .unwrap();

    let mut ext_page_id_iter = stmt.query_map(&[&ext_page_id.to_string(), &title.to_string()],
                   |row| row.get(0))
        .unwrap();

    let ext_page_id_opt: Option<rusqlite::Result<String>> = ext_page_id_iter.next();

    match ext_page_id_opt {
        Some(Ok(_)) => (),
        Some(err) => {
            err.unwrap();
        }
        None => {
            conn.execute("INSERT INTO page (title, ext_page_id) VALUES (?1, ?2)",
                         &[&title.to_string(), &ext_page_id.to_string()])
                .unwrap();
        }
    }
}

fn add_to_database(conn: &Connection, base: &JsonValue) -> Result<(), WikiRacerError> {
    match base {
        &JsonValue::Object(ref base_obj) => {
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
                                        let page_id = page_entry.0;
                                        let page = page_entry.1;
                                        // println!("{:?} --- {:?}", page_id, page);
                                        match page {
                                            &JsonValue::Object(ref page_obj) => {
                                                // println!("{:?}", page_obj);
                                                if let Some(page_title) = page_obj.get("title") {
                                                    // println!("{:?}", page_title);
                                                    match page_title {
                                                        &JsonValue::String(ref page_title_string) => {
                                                            println!("{:?}", page_title_string);
                                                            let title_str = page_title_string.as_str();
                                                            add_page_to_database(conn, Some(title_str), page_id);
                                                        }
                                                        &JsonValue::Short(ref page_title_short) => {
                                                            println!("{:?}", page_title_short);
                                                            let title_str =
                                                                page_title_short.as_str();
                                                            add_page_to_database(conn,
                                                                                 Some(title_str),
                                                                                 page_id);
                                                        }
                                                        _ => return Err(WikiRacerError::FormatError("Page Title was not a string or short")),
                                                    }
                                                }
                                                if let Some(page_links_here) =
                                                    page_obj.get("linkshere") {
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
                                                                                        let other_page_id_str = other_page_id_string.as_str();
                                                                                        add_page_to_database(conn, None, other_page_id_str);
                                                                                        add_link_to_database(conn, page_id, other_page_id_str);
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
                                            _ => {
                                                return Err(WikiRacerError::FormatError("Page was not an object"))
                                            }
                                        }
                                    }
                                }
                                _ => {
                                    return Err(WikiRacerError::FormatError("Pages was not an \
                                                                            object"))
                                }
                            }
                        }
                    }
                    _ => return Err(WikiRacerError::FormatError("Query was not an object")),
                }
            }
        }
        _ => return Err(WikiRacerError::FormatError("Base was not an object")),
    }
    Ok(())
}

fn main() {
    // let conn = create_database();
    let conn = open_database();

    let ssl = NativeTlsClient::new().unwrap();
    let connector = HttpsConnector::new(ssl);
    let client = Client::with_connector(connector);

    let mut resp = client.get("https://en.wikipedia.org/w/api.\
              php?action=query&format=json&titles=Jesus&prop=linkshere&lhprop=pageid")
        .send()
        .unwrap();
    let mut body = vec![];
    resp.read_to_end(&mut body).unwrap();
    let string = String::from_utf8_lossy(&body);
    let parsed = json::parse(&string).unwrap();
    // println!("{}", parsed);
    add_to_database(&conn, &parsed).unwrap();
}
