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
            let diffs: Vec<i16> = std::iter::zip(firsts, seconds)
                .map(|(a, b)| b - a)
                .collect();
            futures::stream::iter(diffs)
        })
        .flatten()
        .enumerate()
        .fold([0u64; 200], |mut counts, (i, diff)| async move {
            if diff < 200 {
                counts[diff as usize] += 1;
            } else {
                counts[199] += 1;
            }
            if i % 100000 == 0 {
                eprintln!("counts: {}", i);
                for (n, c) in counts.iter().enumerate() {
                    eprintln!("{}: {}", n, ".".repeat(*c as usize / 1_000))
                }
            }
            counts
        })
        .await;
    eprintln!("results: {:?}", results);
    Ok(())
}
