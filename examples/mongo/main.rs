use mongodb::{
    bson::{doc, Document},
    options::ClientOptions,
    Client, Collection,
};

#[tokio::main]
async fn main() -> mongodb::error::Result<()> {
    let client_options = ClientOptions::parse("mongodb://localhost:27017").await?;
    let client = Client::with_options(client_options)?;

    // List the names of the databases
    // for db_name in client.list_database_names(None, None).await? {
    //     println!("{}", db_name);
    // }

    let db = client.database("isoexpress");
    let coll: Collection<Document> = db.collection("da_lmp_hourly");
    let hub = coll
        .find_one(doc! {"date": "2024-01-01", "ptid": 4000})
        .await?;
    println!("Found the hub price:\n{:#?}", hub);

    Ok(())
}
