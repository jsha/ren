#![feature(slice_as_chunks)]
#![feature(slice_partition_dedup)]
use futures::StreamExt;
use sqlx::postgres::PgPoolOptions;

#[derive(sqlx::FromRow, Debug)]
struct Issuance {
    #[allow(dead_code)]
    name: String,
    issuances: Vec<u8>,
}

#[derive(Copy, Clone, Debug)]
enum RenewalType {
    Timely = 0,
    Rescued = 1,
    Expired = 2,
}

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    env_logger::init();
    let pool = PgPoolOptions::new()
        .connect(&std::env::var("DSN").expect("didn't find $DSN environment variable"))
        .await?;
    let results = sqlx::query_as::<_, Issuance>("SELECT * FROM issuance")
        .fetch_many(&pool)
        .map(move |f| {
            let f = f.unwrap().expect_right("expected value");
            let mut issuances: Vec<_> = f
                .issuances
                .as_chunks()
                .0
                .iter()
                .map(|x| i16::from_be_bytes(*x))
                .collect();
            issuances.sort();
            let deduped = issuances.partition_dedup().0.to_vec();
            let firsts = deduped.iter();
            let seconds = deduped.iter().skip(1);
            use RenewalType::*;
            let diffs: Vec<RenewalType> = std::iter::zip(firsts, seconds)
                .map(|(a, b)| match b - a {
                    i16::MIN..=70 => Timely,
                    71..=71 => Rescued,
                    71.. => Expired,
                })
                .collect();
            let counts = diffs.iter().fold([0u16; 3], |mut counts, rt| {
                counts[*rt as usize] += 1;
                counts
            });
            let kind = if counts[0] > 2 && counts[2] == 0 && counts[1] > 0 {
                eprintln!("{}: {} timely, {} rescued", f.name, counts[0], counts[1]);
                Some((counts[0], counts[1]))
            } else {
                None
            };
            futures::stream::iter(kind)
        })
        .flatten()
        .collect::<Vec<_>>()
        .await;
    eprintln!("results: {:?}", results);
    Ok(())
}
