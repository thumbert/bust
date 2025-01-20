use std::{
    env,
    error::Error,
    fmt::Display,
    fs::{self, File},
    io,
    path::Path,
    process::Command,
};

use reqwest::{
    blocking::Client,
    header::{ACCEPT, UPGRADE_INSECURE_REQUESTS, USER_AGENT},
    StatusCode,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportError {
    Empty,
    Incomplete,
}

impl Display for ReportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use ReportError::*;
        match self {
            Empty => write!(f, "File is empty"),
            Incomplete => write!(f, "File is incomplete"),
        }
    }
}


pub fn download_file(
    url: String,
    require_auth: bool,
    accept_header: Option<String>,
    file_path: &Path,
    gzip: bool,
) -> Result<(), Box<dyn Error>> {
    let client = Client::new();
    let mut builder = client
        .get(url)
        .header(USER_AGENT, "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
        .header(UPGRADE_INSECURE_REQUESTS, "1");
    if let Some(accept_header) = accept_header {
        builder = builder.header(ACCEPT, accept_header);
    }
    if require_auth {
        let user_name = env::var("ISONE_WS_USER").unwrap();
        let password = env::var("ISONE_WS_PASSWORD").unwrap();
        builder = builder.basic_auth(user_name, Some(password));
    }
    let response = builder.send();
    // println!("{:?}", response);
    if response.as_ref().unwrap().status() != StatusCode::OK {
        return Err(Box::from(format!("Download failed! {:?}", response)));
    }
    let body = response.unwrap().text().expect("invalid body");
    // println!("{}", body);

    let dir = file_path.parent().unwrap();
    let _ = fs::create_dir_all(dir);
    let mut out = File::create(file_path).expect("failed to create file");
    io::copy(&mut body.as_bytes(), &mut out).expect("failed to copy content");

    // gzip it
    if gzip {
        Command::new("gzip")
            .args(["-f", file_path.to_str().unwrap()])
            .current_dir(dir)
            .spawn()
            .unwrap()
            .wait()
            .expect("gzip failed");
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::error::Error;

    #[ignore]
    #[test]
    fn download_file_test() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        download_file(
            "https://webservices.iso-ne.com/api/v1.1/singlesrccontingencylimits/day/20250112"
                .to_string(),
            true,
            Some("application/json".to_string()),
            Path::new("/home/adrian/Downloads/Archive/IsoExpress/SingleSourceContingency/Raw/2025/ssc_20250112.json"),
            true
        )?;
        Ok(())
    }
}
