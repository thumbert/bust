use std::{fs, process::Command};

use actix_web::{post, web, HttpResponse, Responder};
use serde_json::json;

#[post("/admin/jobs/log/{job_name}")]
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
    let script_name = format!(
        "/home/adrian/Documents/jobs/{}.sh",
        name.replace("-", "_")
    );
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

