use rusqlite::Connection;

pub fn create_database() -> Connection {
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

pub fn open_database() -> Connection {
    Connection::open("database.sqlite3").unwrap()
}

#[derive(Debug)]
pub enum WikiRacerError {
    FormatError(&'static str),
}
