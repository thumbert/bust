use pest::Parser;
use pest_derive::Parser;

use super::Interval;

#[derive(Parser)]
#[grammar = "grammars/term.pest"]
pub struct TermParser;

fn parse_term(input: &str) -> Result<Interval, Error<Rule>> {
    let t = TermParser::parse(Rule::term, input)
        .expect("unsuccessful parse")
        .next().unwrap();

    for token in t.into_inner() {
        match token.as_rule() {
            Rule::simple => (),
            Rule::EOI => (),
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use pest::Parser;

    use crate::interval::term::{Rule, TermParser};

    #[test]
    fn test_parse_term() {
        // month
        assert!(TermParser::parse(Rule::term, "Jan23").is_ok());
        assert!(TermParser::parse(Rule::term, "JAN23").is_ok());
        assert!(TermParser::parse(Rule::term, "JaN23").is_ok());
        assert!(TermParser::parse(Rule::term, "January23").is_ok());
        assert!(TermParser::parse(Rule::term, "Jan2023").is_ok());
        assert!(TermParser::parse(Rule::term, "Janu2023").is_err());
        // year
        assert!(TermParser::parse(Rule::term, "2023").is_ok());
        assert!(TermParser::parse(Rule::term, "23").is_err());
    }
}
