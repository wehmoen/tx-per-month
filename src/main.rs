use std::collections::HashMap;
use mongodb::bson::{DateTime, doc};
use serde::{Deserialize, Serialize};

type Year = i64;

type RoninChainStatistics = [i64; 12];

fn create_ronin_chain_statistics() -> RoninChainStatistics {
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
}

#[derive(Serialize, Deserialize)]
struct RoninTransaction {
    created_at: DateTime,
}

#[tokio::main]
async fn main() {
    let client = mongodb::Client::with_uri_str("mongodb://localhost:27017").await.expect("Failed to create database connection!");
    let database = client.database("ronin");
    let collection = database.collection::<RoninTransaction>("transactions");

    let mut statistics: HashMap<Year, RoninChainStatistics> = HashMap::new();

    let years: [Year; 3] = [2020, 2021, 2022];

    let months: Vec<i64> = (1..=12).collect();

    for year in years {
        let mut this_year = create_ronin_chain_statistics();
        for month in months.iter() {
            println!("Year: {:<5}Month: {:<2}", year, month);
            let index = (month - 1) as usize;

            let res = collection.count_documents(doc! {
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

            }, None).await.unwrap();

            this_year[index] = res as i64;
        }

        statistics.insert(year, this_year);
    }

    std::fs::write("tx-per-month.json", serde_json::to_string(&statistics).unwrap()).unwrap();

    println!("Results stored as: tx-per-month.json");
}
