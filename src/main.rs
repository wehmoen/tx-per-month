use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use  futures::Stream;
use futures::stream::{StreamExt};
use mongodb::bson::{DateTime, doc};
use mongodb::options::FindOptions;

type Year = u64;

#[derive(Default, Serialize, Deserialize)]
struct RoninChainStatistics {
    january: u64,
    february: u64,
    march: u64,
    april: u64,
    may: u64,
    june: u64,
    july: u64,
    august: u64,
    september: u64,
    october: u64,
    november: u64,
    december: u64,
}

#[derive(Serialize, Deserialize)]
struct RoninTransaction {
    created_at: DateTime
}

#[tokio::main]
async fn main() {

    let client = mongodb::Client::with_uri_str("mongodb://localhost:27017").await.expect("Failed to create database connection!");
    let database = client.database("ronin");
    let collection = database.collection::<RoninTransaction>("transactions");

    let mut statistics : HashMap<Year, RoninTransaction> = HashMap::new();

    let options = FindOptions::builder()
        .no_cursor_timeout(Some(true))
        .batch_size(Some(100u32))
        .sort(doc! {
                "created_at": 1i64
            })
        .limit(5)
        .build();

    let mut cursor = collection.find(None, options).await.unwrap();

    while let Some(tx) = cursor.next().await {
            let tx = tx.unwrap();
            let date_parts = tx.created_at.to_string();

        println!("{}", date_parts);

    }


    println!("Export!");
}
