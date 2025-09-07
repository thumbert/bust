// struct Domino(u8, u8);

// struct Cell {
//     id: char,
//     top: Box<Option<Cell>>,
//     right: Box<Option<Cell>>,
//     bottom: Box<Option<Cell>>,
//     left: Box<Option<Cell>>,
// }

// trait Constraint {}
// struct ConstraintEqual {
//     cellIds: Vec<char>,
// }
// impl Constraint for ConstraintEqual {}

// struct Puzzle<K: Constraint> {
//     cells: Vec<Cell>,
//     dominoes: Vec<Domino>,
//     constraints: Vec<K>,
// }

// #[cfg(test)]
// mod tests {
//     use crate::math::puzzles::pips::*;

//     #[test]
//     fn puzzle0() {
//         let dominoes = vec![Domino(0, 5), Domino(4, 4), Domino(0, 2), Domino(3, 5)];
//         let a = Cell {
//             id: 'a',
//             top: Box::new(None),
//             left: Box::new(None),
//             bottom: Box::new(b),
//             right: Box::new(None),
//         };
//         let cells = vec![];

//         let puzzle = Puzzle {
//             cells: todo!(),
//             dominoes,
//             constraints: todo!(),
//         };
//     }
// }
