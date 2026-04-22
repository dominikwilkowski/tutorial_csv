use std::path::PathBuf;

mod csv_simple;
mod csv_sliding_window;

fn main() {
	println!("csv_simple:\n{:?}", csv_simple::parse_csv(PathBuf::from("test.csv")));
	println!("csv_sliding_window:\n{:?}", csv_sliding_window::parse_csv(PathBuf::from("test.csv")));
}
