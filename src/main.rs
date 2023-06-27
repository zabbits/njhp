use anyhow::Result;
use rusqlite::{Connection, params};
use scraper::{Html, Selector};
use serde::Serialize;
use time::macros::{date, format_description};

// https://www.njhouse.com.cn/include/everyday/2023/dist20230620.htm
// https://www.njhouse.com.cn/include/everyday/2023/project20230620.htm

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct NjVolume {
    pub name: String,
    pub subscription: u32,
    pub transaction: u32,
    pub day: String,
}

impl NjVolume {
    pub fn new(name: String, subscription: u32, transaction: u32, day: String) -> Self {
        Self { name, subscription, transaction, day }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut st = date!(2020-01-01);
    let ed = date!(2023-06-27);
    let conn = Connection::open("./res/njhp.db")?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS njhp (
            name TEXT NOT NULL,
            sub INTEGER NOT NULL,
            trans INTEGER NOT NULL,
            day TEXT NOT NULL
        )", [])?;
    while st < ed {
        let day = st.clone().format(format_description!("[year][month][day]"))?;
        let s = raw_text_by_day(st.year(), &day).await?;
        let vals = from_str(&s, &day)?;
        save(&conn, vals)?;
        st = st.next_day().unwrap();
    }

    Ok(())
}

async fn raw_text_by_day(year: i32, day: &str) -> Result<String> {
    let url = format!("https://www.njhouse.com.cn/include/everyday/{year}/dist{day}.htm");

    Ok(reqwest::get(url).await?.text().await?)
}

fn from_str(s: &str, day: &str) -> Result<Vec<NjVolume>> {
    let h = Html::parse_document(s);
    let s = Selector::parse("body tbody tr").unwrap();
    let vals = h.select(&s);
    let s = Selector::parse("td").unwrap();
    let mut ret = vec![];
    for val in vals {
        let td = val.select(&s);
        // let mut v = vec![];
        let v = td.map(|x| x.text().collect::<Vec<_>>()[0]).collect::<Vec<_>>();
        let v = NjVolume::new(v[0].to_owned(), 
            v[4].parse()?, 
            v[5].parse()?,
            day.to_owned(),
        );
        ret.push(v);
    }

    Ok(ret)
}

fn save(conn: &Connection, vals: Vec<NjVolume>) -> Result<()> {
    for val in vals {
        conn.execute(
            "INSERT INTO njhp (name, sub, trans, day) VALUES (?, ?, ?, ?)",
            params![val.name, val.subscription, val.transaction, val.day],
        )?;
    }

    Ok(())
}
