use std::{env, fs, process::Command};

use actix_web::{get, post, web, HttpResponse, Responder};
use serde_json::json;

#[get("/admin/jobs/job-names")]
async fn api_get_job_names() -> impl Responder {
    let msg = format!(
        "Could not find directory: {}",
        env::var("JOBS_DIR").unwrap()
    );
    let paths = fs::read_dir(env::var("JOBS_DIR").unwrap()).expect(&msg);
    let mut job_names = Vec::new();
    for path in paths {
        let path = path.expect(&msg);
        let file_name = path.file_name();
        let file_name = file_name.to_string_lossy().to_string();
        if file_name.ends_with(".sh") {
            job_names.push(file_name);
        }
    }
    job_names.sort();
    HttpResponse::Ok().json(json!(job_names))
}

#[get("/admin/jobs/log/{job_name}")]
async fn api_get_log(path: web::Path<String>) -> impl Responder {
    let name = path.into_inner();
    let log_file = format!(
        "/home/adrian/Documents/jobs/logs/{}.txt",
        name.replace("-", "_")
    );
    let msg = format!("Could not find log file: {}", log_file);
    let contents = fs::read_to_string(&log_file).expect(&msg);
    HttpResponse::Ok().content_type("text/plain").body(contents)
}

#[post("/admin/jobs/run/{job_name}")]
async fn api_run_job(path: web::Path<String>) -> impl Responder {
    let name = path.into_inner();
    let script_name = format!("/home/adrian/Documents/jobs/{}.sh", name.replace("-", "_"));
    // Launch the process and capture the output
    let output = Command::new(script_name).output();
    if output.is_err() {
        return HttpResponse::InternalServerError().body("Failed to start the job!");
    }
    // println!("Job {} started!", name);

    let output = output.unwrap();
    // println!("Job status: {}", output.status);

    let stdout = String::from_utf8(output.stdout).unwrap_or_else(|_| "Invalid output".to_string());
    // println!("Job output: {}", stdout);

    let stderr = String::from_utf8(output.stderr).unwrap_or_else(|_| "Invalid output".to_string());
    // println!("Job error: {}", stderr);

    HttpResponse::Ok().json(json!({
        "job_name": name,
        "exit status": output.status.code(),
        "stdout": stdout,
        "stderr": stderr,
    }))
}

#[cfg(test)]
mod tests {
    use std::{env, path::Path};

    #[test]
    fn api_get_job_names() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/admin/jobs/job-names",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        let vs: Vec<String> = serde_json::from_str(&response).unwrap();
        println!("{:?}", vs);
        assert_eq!(vs.len(), 10);
        Ok(())
    }


    #[test]
    fn api_get_log() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/admin/jobs/log/update-nrc-generator-status",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        assert!(response.contains("Downloaded file successfully"));
        Ok(())
    }
}
