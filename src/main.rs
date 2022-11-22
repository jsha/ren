use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

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
        .max_connections(50)
        .connect(&std::env::var("DSN").expect("didn't find $DSN environment variable"))
        .await?;

    let name = std::env::args().nth(1).unwrap();
    let val: i16 = std::env::args().nth(2).unwrap().parse().unwrap();
    add_issuance(&pool, &name, val).await?;

    Ok(())
}

async fn add_issuance(pool: &Pool<Postgres>, name: &str, n: i16) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    drop(pool); // Make sure we don't accidentally use the pool instead of the tx.
    let issuance = sqlx::query_as::<_, Issuance>("SELECT * FROM issuance WHERE name = $1")
        .bind(name)
        .fetch_optional(&mut tx)
        .await?;

    match issuance {
        Some(mut issuance) => {
            issuance
                .issuances
                .extend_from_slice(n.to_be_bytes().as_ref());
            sqlx::query("UPDATE issuance SET issuances = $1 WHERE name = $2")
                .bind(issuance.issuances)
                .bind(name)
                .execute(&mut tx)
                .await?;
            tx.commit().await?;

            Ok(())
        }
        None => {
            sqlx::query("INSERT INTO issuance (name, issuances) VALUES ($1, $2)")
                .bind(name)
                .bind(&n.to_be_bytes())
                .execute(&mut tx)
                .await?;
            tx.commit().await?;
            Ok(())
        }
    }
}
