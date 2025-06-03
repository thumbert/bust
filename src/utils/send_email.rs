use std::{env, path::Path};

use reqwest::Response;
use serde_json::json;

/// Send an email using the Mailtrap API
///
///
pub async fn send_email(
    from: String,
    to: Vec<String>,
    subject: String,
    text: String,
    html: Option<String>,
) -> Result<Response, reqwest::Error> {
    dotenvy::from_path(Path::new(".env/prod.env")).unwrap();
    let api_url = "https://send.api.mailtrap.io/api/send";
    let api_key = env::var("MAILTRAP_API_KEY").unwrap();

    let to_ = to
        .iter()
        .map(|email| json!({"email": email}))
        .collect::<Vec<_>>();
    let email_payload = json!({
        "from": {"email" : from},
        "to": to_,
        "subject": subject,
        "text": text,
        "html": html,
    });

    let client = reqwest::Client::new();
    let response = client
        .post(api_url)
        .header("Content-Type", "application/json")
        .header("Api-Token", api_key)
        .body(email_payload.to_string())
        .send()
        .await?;

    Ok(response)
}

#[cfg(test)]
mod tests {
    use std::{env, error::Error, path::Path};

    use build_html::{Html, Table};

    use super::send_email;

    #[ignore]
    #[tokio::test]
    async fn email_test() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();

        let res = send_email(
            env::var("EMAIL_FROM").unwrap(),
            vec![env::var("EMAIL_MAIN").unwrap()],
            "Plain email test".into(),
            "This is a test email using Rust and Mailtrap API!".into(),
            None,
        )
        .await?;
        println!("{:?}", res);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn email_html_test() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();

        let source_table = [[1, 2, 3], [4, 5, 6], [7, 8, 9]];
        let html_table = Table::from(source_table)
            .with_header_row(["A", "Blue", "Carrot"])
            .to_html_string();

        let res = send_email(
            env::var("EMAIL_FROM").unwrap(),
            vec![env::var("EMAIL_MAIN").unwrap()],
            "Html email test".into(),
            "fallback content".into(),
            Some(html_table),
        )
        .await?;
        println!("{:?}", res);

        Ok(())
    }
}
