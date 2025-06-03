use regex::Regex;


fn main() {
    let input = "2025-05-07  12:51:06_example";
    
    // Replace all occurrences of '-', ':' or '_' with a space
    let re = Regex::new(r"[-:_ ]").unwrap();
    let result = re.replace_all(input, "");
    
    println!("Original: {}", input);
    println!("Modified: {}", result);
    
    // If you want to remove these characters entirely (replace with empty string)
    let removed = re.replace_all(input, "");
    println!("With characters removed: {}", removed);
}