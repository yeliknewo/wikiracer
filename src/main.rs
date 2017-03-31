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
                    length          INTEGER NOT NULL,
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

fn add_link_to_database(conn: &Connection, to_ext_page_id: String, from_ext_page_id: String) {
    let to_page_id_opt = get_page_id(conn, to_ext_page_id);
    let from_page_id_opt = get_page_id(conn, from_ext_page_id);

    if let Some(to_page_id) = to_page_id_opt {
        if let Some(from_page_id) = from_page_id_opt {
            if !has_link(conn, to_page_id, from_page_id) {
                conn.execute("INSERT INTO link (to_page_id, from_page_id, length) VALUES \
                              (?1, ?2, 1)",
                             &[&to_page_id, &from_page_id])
                    .unwrap();
            }
        }
    }
}

fn has_link(conn: &Connection, to_page_id: i64, from_page_id: i64) -> bool {
    let mut stmt =
        conn.prepare("SELECT link_id FROM link WHERE to_page_id = ?1 and from_page_id = ?2")
            .unwrap();

    let link_id_iter = stmt.query_map(&[&to_page_id, &from_page_id], |row| row.get(0)).unwrap();

    for link_id_result in link_id_iter {
        let _: i64 = link_id_result.unwrap();
        return true;
    }
    false
}

fn get_next_page_id_target(conn: &Connection) -> Option<String> {
    let mut stmt = conn.prepare("SELECT ext_page_id FROM page WHERE title IS NULL").unwrap();

    let ext_page_id_iter = stmt.query_map(&[], |row| row.get(0)).unwrap();

    for ext_page_id_result in ext_page_id_iter {
        return Some(ext_page_id_result.unwrap());
    }
    None
}

fn get_page_id(conn: &Connection, ext_page_id: String) -> Option<i64> {
    let mut stmt = conn.prepare("SELECT page_id FROM page WHERE ext_page_id = ?1").unwrap();

    let page_id_iter = stmt.query_map(&[&ext_page_id.to_string()], |row| row.get(0)).unwrap();

    for page_id_result in page_id_iter {
        return Some(page_id_result.unwrap());
    }
    None
}

fn add_page_to_database(conn: &Connection, title_opt: Option<String>, ext_page_id: String) {
    if let Some(title) = title_opt {
        add_page_to_database_with_title(conn, title, ext_page_id);
    } else {
        add_page_to_database_without_title(conn, ext_page_id);
    }
}

fn add_page_to_database_without_title(conn: &Connection, ext_page_id: String) {
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

fn add_page_title_to_database(conn: &Connection, title: String, ext_page_id: String) {
    conn.execute("UPDATE page SET title = ?1 WHERE ext_page_id = ?2",
                 &[&title.to_string(), &ext_page_id.to_string()])
        .unwrap();
}

fn add_page_to_database_with_title(conn: &Connection, title: String, ext_page_id: String) {
    let mut stmt = conn.prepare("SELECT ext_page_id FROM page WHERE ext_page_id = ?1 or title = ?2")
        .unwrap();

    let mut ext_page_id_iter = stmt.query_map(&[&ext_page_id.to_string(), &title.to_string()],
                   |row| row.get(0))
        .unwrap();

    let ext_page_id_opt: Option<rusqlite::Result<String>> = ext_page_id_iter.next();

    match ext_page_id_opt {
        Some(Ok(_)) => {
            add_page_title_to_database(conn, title, ext_page_id);
        }
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

fn add_to_database(conn: &Connection, base: &JsonValue) -> Result<Option<String>, WikiRacerError> {
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
                                                            if continue_opt.is_none() {
                                                                println!("{:?}", page_title_string);
                                                            }
                                                            let title_str = page_title_string.to_string();
                                                            add_page_to_database(conn, Some(title_str), page_id.clone());
                                                        }
                                                        &JsonValue::Short(ref page_title_short) => {
                                                            if continue_opt.is_none() {
                                                                println!("{:?}", page_title_short);
                                                            }
                                                            let title_str =
                                                                page_title_short.to_string();
                                                            add_page_to_database(conn,
                                                                                 Some(title_str),
                                                                                 page_id.clone());
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
                                                                                        let other_page_id_str = other_page_id_string.to_string();
                                                                                        add_page_to_database(conn, None, other_page_id_str.clone());
                                                                                        add_link_to_database(conn, page_id.clone(), other_page_id_str.clone());
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
    Ok(continue_opt)
}


fn run_next_query(client: &Client, conn: &Connection) {
    let pageids = get_next_page_id_target(conn).unwrap();

    run_query(client, conn, pageids);
}

fn run_query(client: &Client, conn: &Connection, pageids: String) {
    let mut parsed = query_wikipedia(client, pageids.clone(), None);

    while let Some(json_continue) = add_to_database(&conn, &parsed).unwrap() {
        parsed = query_wikipedia(client, pageids.clone(), Some(json_continue));
    }
}

fn query_wikipedia(client: &Client, pageids: String, continue_opt: Option<String>) -> JsonValue {
    let url = {
        if let Some(continue_string) = continue_opt {
            format!("https://en.wikipedia.org/w/api.\
                               php?action=query&format=json&pageids={}&prop=linkshere&lhprop=pageid&lhlimit=500&redirects{}",
                    pageids,
                    continue_string)
        } else {
            format!("https://en.wikipedia.org/w/api.\
                               php?action=query&format=json&pageids={}&prop=linkshere&lhprop=pageid&lhlimt=500&redirects",
                    pageids)
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

fn main() {
    let ssl = NativeTlsClient::new().unwrap();
    let connector = HttpsConnector::new(ssl);
    let client = Client::with_connector(connector);

    let created = false;

    let conn = {
        if created {
            open_database()
        } else {
            let conn = create_database();
            run_query(&client, &conn, "1095706".to_string());
            conn
        }
    };

    loop {
        run_next_query(&client, &conn);
    }
}
