use std::path::PathBuf;

mod csv_simple;

use crate::csv_simple::parse_csv;

fn main() {
	let csv_file = PathBuf::from("test.csv");
	match std::fs::read_to_string(csv_file) {
		Ok(content) => println!("{:#?}", parse_csv(content)),
		Err(error) => eprintln!("An error occurred while reading the CSV file: {error}"),
	}
}
