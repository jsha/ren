use chrono::{prelude::*, Days};

fn main() {
    let mut date = Utc.with_ymd_and_hms(2019, 3, 15, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2022, 11, 16, 0, 0, 0).unwrap();
    while date <= end {
        println!("results-{}.tsv.gz", date.format("%Y-%m-%d"));
        date = date + Days::new(1);
    }
}
