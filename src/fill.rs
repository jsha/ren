#![feature(slice_as_chunks)]
use std::sync::Arc;

use chrono::prelude::*;
use futures::StreamExt;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

#[derive(sqlx::FromRow, Debug)]
struct Issuance {
    #[allow(dead_code)]
    name: String,
    issuances: Vec<u8>,
}

const CONNS: usize = 50;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    env_logger::init();
    let pool = PgPoolOptions::new()
        .max_connections(CONNS as u32)
        .min_connections(100)
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
                let record = result.expect("failed to parse CSV line");
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
            .map(|(name, num_days)| tokio::spawn(add_issuance(pool.clone(), name, num_days))),
    )
    .buffer_unordered(CONNS + 10)
    .collect::<Vec<_>>();
    join_handles.await;
    Ok(())
}

async fn add_issuance(pool: Arc<Pool<Postgres>>, name: String, n: i16) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    drop(pool); // Make sure we don't accidentally use the pool instead of the tx.
    let issuance = sqlx::query_as::<_, Issuance>("SELECT * FROM issuance WHERE name = $1")
        .bind(&name)
        .fetch_optional(&mut tx)
        .await?;

    match issuance {
        Some(mut issuance) => {
            // This entry already exists; no need to write.
            if issuance
                .issuances
                .as_chunks()
                .0
                .iter()
                .map(|x| i16::from_be_bytes(*x))
                .any(|x| x == n)
            {
                return Ok(());
            }

            issuance
                .issuances
                .extend_from_slice(n.to_be_bytes().as_ref());
            sqlx::query("UPDATE issuance SET issuances = $1 WHERE name = $2")
                .bind(issuance.issuances)
                .bind(&name)
                .execute(&mut tx)
                .await?;
            tx.commit().await?;

            Ok(())
        }
        None => {
            sqlx::query("INSERT INTO issuance (name, issuances) VALUES ($1, $2)")
                .bind(&name)
                .bind(&n.to_be_bytes())
                .execute(&mut tx)
                .await?;
            tx.commit().await?;
            Ok(())
        }
    }
}