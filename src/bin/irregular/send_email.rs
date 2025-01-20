use std::{env, path::Path};

use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), reqwest::Error> {
    dotenvy::from_path(Path::new(".env/prod.env")).unwrap();
    let api_url = "https://send.api.mailtrap.io/api/send";
    let api_key = env::var("MAILTRAP_API_KEY").unwrap();
    println!("{}", api_key);
    let email_payload = json!({
        "from": {"email" : "humbert.tony@polywatt.us"},
        "to": [{"email": "humbert.tony@yahoo.com"}],
        "subject": "Test Email",
        "text": "This is a test email using Rust and Mailtrap API!",
    });

    let client = reqwest::Client::new();
    let response = client
        .post(api_url)
        .header("Content-Type", "application/json")
        .header("Api-Token", api_key)
        .body(email_payload.to_string()) // Serialize the JSON payload to a string
        .send()
        .await?;

    if response.status().is_success() {
        println!("Email sent successfully!");
    } else {
        println!("Failed to send email. Status: {:?}", response.status());

        // Print the response body for additional information
        let body = response.text().await?;
        println!("Response body: {}", body);
    }

    Ok(())
}