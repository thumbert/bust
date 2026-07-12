use std::f64::consts::PI;

use bust::interval::month::{month, Month};

trait Shape {
    fn area(&self) -> f64;
}

struct Circle {
    radius: f64,
}

impl Shape for Circle {
    fn area(&self) -> f64 {
        PI * self.radius * self.radius
    }
}

enum ArchiveKind {
    Intermediary(Box<dyn ArchiveIntermediary>),
    Direct(Box<dyn ArchiveDirect>),
}

trait Archive {
    fn name(&self) -> String;
}

trait ArchiveIntermediary: Archive {
    fn update(&self, files: Vec<String>) -> Result<(), Box<dyn std::error::Error>>;
}

trait ArchiveDirect: Archive {
    fn update(&self, month: Month) -> Result<(), Box<dyn std::error::Error>>;
}

struct Archive1;

impl Archive for Archive1 {
    fn name(&self) -> String {
        "Archive1".to_string()
    }
}

impl ArchiveIntermediary for Archive1 {
    fn update(&self, files: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
        println!("Updating Archive1 with files: {:?}", files);
        Ok(())
    }
}

struct Archive2;

impl Archive for Archive2 {
    fn name(&self) -> String {
        "Archive2".to_string()
    }
}

impl ArchiveDirect for Archive2 {
    fn update(&self, month: Month) -> Result<(), Box<dyn std::error::Error>> {
        println!("Updating Archive2 for month: {:?}", month);
        Ok(())
    }
}

fn main() {
    let c = Circle { radius: 1.0 };
    println!("Area: {}", c.area());

    let archives: Vec<ArchiveKind> = vec![
        ArchiveKind::Intermediary(Box::new(Archive1)),
        ArchiveKind::Direct(Box::new(Archive2)),
    ];

    for archive in &archives {
        match archive {
            ArchiveKind::Intermediary(a1) => {
                println!("Archive name: {}", a1.name());
                a1.update(vec!["file1.txt".to_string(), "file2.txt".to_string()])
                    .unwrap();
            }
            ArchiveKind::Direct(a2) => {
                println!("Archive name: {}", a2.name());
                a2.update(month(2024, 6)).unwrap();
            }
        }
    }
}
