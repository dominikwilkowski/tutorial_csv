use std::path::PathBuf;

mod csv_simple;
mod csv_sliding_window;

fn main() {
	println!("csv_simple:\n{:?}", csv_simple::Csv::parse_file(PathBuf::from("small_test.csv")));
	println!(
		"csv_sliding_window:\n{:?}",
		csv_sliding_window::Csv::parse_file(PathBuf::from("small_test.csv"))
			.and_then(|csv| csv.collect::<Result<Vec<_>, _>>())
	);
}
