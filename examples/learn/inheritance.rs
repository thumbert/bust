use std::f64::consts::PI;

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


fn main() {
    let c = Circle {radius: 1.0};
    println!("Area: {}", c.area());
}