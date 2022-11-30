#![feature(slice_as_chunks)]
use std::sync::Arc;

use chrono::prelude::*;
use futures::StreamExt;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

const CONNS: usize = 25;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    env_logger::init();
    let pool = PgPoolOptions::new()
        .max_connections(CONNS as u32)
        .connect(&std::env::var("DSN").expect("didn't find $DSN environment variable"))
        .await?;
    let pool = Arc::new(pool);
    for filename in std::env::args().skip(1) {
        process_file(&filename, pool.clone()).await?;
    }

    Ok(())
}

async fn process_file(filename: &str, pool: Arc<Pool<Postgres>>) -> Result<(), sqlx::Error> {
    let basedate = Utc.with_ymd_and_hms(2015, 9, 14, 0, 0, 0).unwrap();

    let file = std::fs::File::open(&filename).expect(format!("opening file {}", filename).as_str());
    let decoder = flate2::read::MultiGzDecoder::new(file);
    let mut csv_decoder = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .from_reader(decoder);
    let begin = std::time::Instant::now();
    let join_handles = futures::stream::iter(
        csv_decoder
            .records()
            .enumerate()
            .map(|(i, result)| {
                let record = result.expect(&format!("failed to parse CSV line in {}", filename));
                let name = record.get(1).expect("getting column 1");
                let date = record.get(2).expect("getting column 2");
                let parsed = Utc
                    .datetime_from_str(&date, "%Y-%m-%d %H:%M:%S")
                    .expect("parsing datetime");
                let diff = parsed - basedate;
                let num_days: i16 = diff.num_days().try_into().expect("date too far from 2015");
                if i % 1000 == 0 {
                    let elapsed = begin.elapsed().as_secs_f64();
                    let rows_per_sec = i as f64 / elapsed;
                    println!("{} {} {} {}", i, rows_per_sec, name, num_days);
                }
                (name.to_string(), num_days)
            })
            .map(|(name, num_days)| add_issuance(pool.clone(), name, num_days)),
    )
    .buffer_unordered(CONNS)
    .collect::<Vec<_>>();

    join_handles
        .await
        .into_iter()
        .collect::<Result<_, sqlx::Error>>()
}

async fn add_issuance(pool: Arc<Pool<Postgres>>, name: String, n: i16) -> Result<(), sqlx::Error> {
    sqlx::query(
        r##"
            INSERT INTO issuance as iss
            (name, issuances) VALUES ($1, $2)
            ON CONFLICT (name) DO UPDATE
              SET issuances = iss.issuances || EXCLUDED.issuances
              WHERE LENGTH(iss.issuances) < 200;
    "##,
    )
    .bind(&name)
    .bind(n.to_be_bytes().as_ref())
    .execute(pool.as_ref())
    .await
    .map(|_| ())
}
