use std::collections::HashMap;
use thousands::Separable;
use chrono::Datelike;
use futures::stream::StreamExt;
use mongodb::bson::{DateTime, doc};
use mongodb::options::FindOptions;
use ferris_says::*;
use std::io::{ stdout, BufWriter };
use serde::{Deserialize, Serialize};

fn welcome(years: &Vec<Year>) {
    let stdout = stdout();
    let greeting = format!("Ronin Chain Statistic Generator\nYears: {}\nDeveloper: wehmoen", years.clone().iter().map(|y| y.to_string()).collect::<Vec<String>>().join(","));
    let mut writer = BufWriter::new(stdout.lock());
    say(greeting.as_bytes(), 64, &mut writer).unwrap();

}

type Year = i64;

#[derive(Default, Serialize, Deserialize)]
struct MonthlyStatistics {
    transactions: i64,
    active_wallets: i64,
}

type AnnualStatistics = [MonthlyStatistics; 12];

fn create_ronin_chain_statistics() -> AnnualStatistics {
    [
        MonthlyStatistics::default(),
        MonthlyStatistics::default(),
        MonthlyStatistics::default(),
        MonthlyStatistics::default(),
        MonthlyStatistics::default(),
        MonthlyStatistics::default(),
        MonthlyStatistics::default(),
        MonthlyStatistics::default(),
        MonthlyStatistics::default(),
        MonthlyStatistics::default(),
        MonthlyStatistics::default(),
        MonthlyStatistics::default()
    ]
}

#[derive(Serialize, Deserialize)]
struct RoninTransaction {
    from: String,
    to: String,
    created_at: DateTime,
}

fn get_current_year_month() -> (i64, i64) {
    let current_date = chrono::Utc::now();
    let year = current_date.year();
    let month = current_date.month();
    (year.into(), month.into())
}

#[tokio::main]
async fn main() {
    let now = get_current_year_month();

    let client = mongodb::Client::with_uri_str("mongodb://localhost:27017").await.expect("Failed to create database connection!");
    let database = client.database("ronin");
    let collection = database.collection::<RoninTransaction>("transactions");

    let mut statistics: HashMap<Year, AnnualStatistics> = HashMap::new();

    let years: Vec<Year> = (2021..=now.0).collect();

    let months: Vec<i64> = (1..=12).collect();

    let options = FindOptions::builder()
        .no_cursor_timeout(Some(true))
        .batch_size(Some(1000u32))
        .sort(doc! {
                "created_at": 1i64
            })
        .build();


    welcome(&years);

    for year in years {
        let mut this_year = create_ronin_chain_statistics();
        for month in months.iter() {
            let index = (month - 1) as usize;

            let mut tx_count: i64 = 0;
            let mut wallets: Vec<String> = vec![];

            if year == now.0 && month >= &now.1 {
                this_year[index] = MonthlyStatistics::default();
            } else {
                let mut cursor = collection.find(doc! {
               "$and": [
                    {
                        "$expr": {
                            "$eq": [
                                {
                                    "$year": "$created_at"
                            },
                            year
                            ]
                        }
                    },
                    {
                        "$expr": {
                            "$eq": [
                                {
                                    "$month": "$created_at"
                            },
                            month
                            ]
                        }
                    },

                ]

                }, options.clone()).await.unwrap();

                while let Some(tx) = cursor.next().await {

                    tx_count += 1;

                    let tx = tx.unwrap();

                    if wallets.contains(&tx.from) {
                        wallets.push(tx.from)
                    }

                    if wallets.contains(&tx.to) {
                        wallets.push(tx.to)
                    }

                    println!("Year: {:<5}Month: {:<2}\tTransactions: {:<12}Wallts: {:<9}", year, month, tx_count.separate_with_commas(), wallets.len().separate_with_commas());
                }

                this_year[index] = MonthlyStatistics {
                    transactions: tx_count,
                    active_wallets: wallets.len() as i64,
                };
            }
        }

        statistics.insert(year, this_year);
    }

    std::fs::write("tx-per-month.json", serde_json::to_string(&statistics).unwrap()).unwrap();

    println!("Results stored as: tx-per-month.json");
}
