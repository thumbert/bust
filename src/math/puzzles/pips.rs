use std::{
    cell,
    collections::{HashMap, HashSet},
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Domino((i32, i32));

impl Domino {
    fn flip(&self) -> Domino {
        Domino((self.0 .1, self.0 .0))
    }
    fn is_double(&self) -> bool {
        self.0 .0 == self.0 .1
    }
}

// Generate all permutations of dominoes, including flipped versions for non-doubles
fn permutations(dominoes: &[Domino]) -> Vec<Vec<Domino>> {
    use itertools::Itertools;
    let mut results = Vec::new();
    for perm in dominoes.iter().permutations(dominoes.len()) {
        // For each permutation, consider flipping non-double dominoes
        let mut flips = vec![vec![]];
        for d in perm {
            let mut new_flips = Vec::new();
            for f in &flips {
                let mut f1 = f.clone();
                f1.push(d.clone());
                new_flips.push(f1);
                if !d.is_double() {
                    let mut f2 = f.clone();
                    f2.push(d.flip());
                    new_flips.push(f2);
                }
            }
            flips = new_flips;
        }
        results.extend(flips);
    }
    results
}

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
enum ConstraintState {
    Satisfied,
    Unsatisfied,
    Undecidable,
}

enum Constraint {
    Equal { cell_ids: Vec<char> },
    NotEqual { cell_ids: Vec<char> },
    Sum { cell_ids: Vec<char>, total: i32 },
}

impl Constraint {
    fn is_satisfied(&self, filled_cells: &HashMap<char, i32>) -> ConstraintState {
        match self {
            Constraint::Equal { cell_ids } => {
                if cell_ids.iter().any(|id| !filled_cells.contains_key(id)) {
                    return ConstraintState::Undecidable;
                }
                let values: HashSet<_> = cell_ids.iter().map(|id| filled_cells[id]).collect();
                if values.len() == 1 {
                    ConstraintState::Satisfied
                } else {
                    ConstraintState::Unsatisfied
                }
            }
            Constraint::NotEqual { cell_ids } => {
                if cell_ids.iter().any(|id| !filled_cells.contains_key(id)) {
                    return ConstraintState::Undecidable;
                }
                let values: HashSet<_> = cell_ids.iter().map(|id| filled_cells[id]).collect();
                if values.len() == cell_ids.len() {
                    ConstraintState::Satisfied
                } else {
                    ConstraintState::Unsatisfied
                }
            }
            Constraint::Sum { cell_ids, total } => {
                if cell_ids.iter().any(|id| !filled_cells.contains_key(id)) {
                    return ConstraintState::Undecidable;
                }
                let sum: i32 = cell_ids.iter().map(|id| filled_cells[id]).sum();
                if sum == *total {
                    ConstraintState::Satisfied
                } else {
                    ConstraintState::Unsatisfied
                }
            }
        }
    }
}

struct Puzzle {
    links: Vec<(char, char)>,
    dominoes: Vec<Domino>,
    constraints: Vec<Constraint>,
    nodes: Vec<char>,
}

impl Puzzle {
    fn new(links: Vec<(char, char)>, dominoes: Vec<Domino>, constraints: Vec<Constraint>) -> Self {
        let mut nodes: Vec<char> = links.iter().flat_map(|(a, b)| vec![*a, *b]).collect();
        nodes.sort();
        nodes.dedup();
        Puzzle {
            links,
            dominoes,
            constraints,
            nodes,
        }
    }

    fn solve(&self) -> Option<HashMap<char, i32>> {
        println!("There are {} candidates to check...", self.count_candidates());
        let all = permutations(&self.dominoes);
        for permutation in all {
            let values: Vec<i32> = permutation
                .iter()
                .flat_map(|d| vec![d.0 .0, d.0 .1])
                .collect();
            if values.len() != self.nodes.len() {
                continue;
            }
            let candidate: HashMap<char, i32> = self.nodes.iter().cloned().zip(values).collect();
            if self.check_solution(&candidate) {
                return Some(candidate);
            }
        }
        None
    }

    fn check_solution(&self, solution: &HashMap<char, i32>) -> bool {
        self.constraints
            .iter()
            .all(|c| c.is_satisfied(solution) == ConstraintState::Satisfied)
    }

    fn check_puzzle(&self) -> bool {
        if self.nodes.len() != self.dominoes.len() * 2 {
            println!(
                "Number of nodes {} does not match number of dominoes {}.",
                self.nodes.len(),
                self.dominoes.len()
            );
            return false;
        }
        for chunk in self.nodes.chunks(2) {
            if chunk.len() == 2 {
                let a = &chunk[0];
                let b = &chunk[1];
                let idx_a = self.nodes.iter().position(|x| x == a).unwrap();
                let idx_b = self.nodes.iter().position(|x| x == b).unwrap();
                if idx_b as i32 - idx_a as i32 != 1 {
                    println!(
                        "Nodes ({}, {}) should be adjacent because they represent a domino.",
                        a, b
                    );
                    return false;
                }
            }
        }
        true
    }

    fn factorial_iterative(&self, n: u64) -> u64 {
        let mut result = 1;
        for i in 1..=n {
            result *= i;
        }
        result
    }

    fn count_candidates(&self) -> u64 {
        let n = self.factorial_iterative(self.dominoes.len() as u64);
        n * 2u64.pow((self.dominoes.iter().filter(|d| !d.is_double()).count()) as u32)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[ignore]
    #[test]
    fn puzzle_20250916() -> Result<(), Box<dyn Error>> {
        let links = vec![
            ('a', 'b'),
            ('b', 'c'),
            ('c', 'd'),
            ('d', 'e'),
            ('e', 'f'),
            ('f', 'g'),
            ('g', 'h'),
            ('h', 'i'),
            ('i', 'j'),
            ('j', 'k'),
            ('k', 'l'),
            ('l', 'a'),
        ];
        let constraints: Vec<Constraint> = vec![
            Constraint::Equal {
                cell_ids: vec!['b', 'c', 'd', 'e'],
            },
            Constraint::Sum {
                cell_ids: vec!['f', 'g'],
                total: 11,
            },
            Constraint::Sum {
                cell_ids: vec!['h', 'i'],
                total: 4,
            },
        ];
        let dominoes = vec![
            Domino((4, 2)),
            Domino((6, 1)),
            Domino((4, 4)),
            Domino((1, 1)),
            Domino((1, 5)),
            Domino((2, 6)),
        ];
        let puzzle = Puzzle::new(links, dominoes, constraints);
        assert!(puzzle.check_puzzle());
        let solution = puzzle.solve();
        println!("Solution: {:?}", solution);

        let mut true_solution: HashMap<char, i32> = HashMap::new();
        true_solution.insert('a', 6);
        true_solution.insert('b', 1);
        true_solution.insert('c', 1);
        true_solution.insert('d',1);
        true_solution.insert('e', 1);
        true_solution.insert('f', 5);
        true_solution.insert('g', 6);
        true_solution.insert('h', 2);
        true_solution.insert('i', 2);
        true_solution.insert('j', 4);
        true_solution.insert('k', 4);
        true_solution.insert('l', 4);
        println!("{:?}", puzzle.check_solution(&true_solution));
        assert_eq!(solution, Some(true_solution));
    

        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}
