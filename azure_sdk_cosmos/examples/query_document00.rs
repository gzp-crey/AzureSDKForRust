use azure_sdk_cosmos::prelude::*;
use std::error::Error;
#[macro_use]
extern crate serde_derive;

#[derive(Serialize, Deserialize, Debug)]
struct MySampleStructOwned {
    id: String,
    a_string: String,
    a_number: u64,
    a_timestamp: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let database_name = std::env::args()
        .nth(1)
        .expect("please specify database name as first command line parameter");
    let collection_name = std::env::args()
        .nth(2)
        .expect("please specify collection name as second command line parameter");
    let query = std::env::args()
        .nth(3)
        .expect("please specify requested query");

    let account = std::env::var("COSMOS_ACCOUNT").expect("Set env variable COSMOS_ACCOUNT first!");
    let master_key =
        std::env::var("COSMOS_MASTER_KEY").expect("Set env variable COSMOS_MASTER_KEY first!");

    let authorization_token = AuthorizationToken::new(account, TokenType::Master, &master_key)?;

    let client = ClientBuilder::new(authorization_token)?;

    let ret = client
        .query_documents(
            &database_name,
            &collection_name,
            Query::from(query.as_ref()),
        )
        .execute_json()
        .await?;

    println!("As JSON:\n{:?}", ret);

    for doc in ret.results {
        println!("{}", doc.result);
    }

    let ret = client
        .query_documents(
            &database_name,
            &collection_name,
            Query::from(query.as_ref()),
        )
        .execute::<MySampleStructOwned>()
        .await?;

    println!("\nAs entities:\n{:?}", ret);

    for doc in ret.results {
        println!("{:?}", doc);
    }

    // test continuation token
    // only if we have more than 2 records
    let ret = client
        .query_documents(
            &database_name,
            &collection_name,
            Query::from(query.as_ref()),
        )
        .max_item_count(2u64)
        .execute::<MySampleStructOwned>()
        .await?;

    println!(
        "Received {} entries. Continuation token is == {:?}",
        ret.results.len(),
        ret.additional_headers.continuation_token
    );

    if let Some(ct) = ret.additional_headers.continuation_token {
        let ret = {
            // if we have more, let's get them
            client
                .query_documents(
                    &database_name,
                    &collection_name,
                    Query::from(query.as_ref()),
                )
                .continuation_token(ct)
                .execute::<MySampleStructOwned>()
                .await?
        };
        println!(
            "Received {} entries. Continuation token is == {:?}",
            ret.results.len(),
            ret.additional_headers.continuation_token
        );
    }

    Ok(())
}
