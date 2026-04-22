use std::{mem::take, path::PathBuf};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CsvParseError {
	UnableToOpenFile,
	UnterminatedQuote,
}

pub fn parse_csv(csv_file: PathBuf) -> Result<Vec<Vec<String>>, CsvParseError> {
	match std::fs::read_to_string(csv_file) {
		Ok(content) => parse(content),
		Err(_) => Err(CsvParseError::UnableToOpenFile),
	}
}

fn parse(content: String) -> Result<Vec<Vec<String>>, CsvParseError> {
	let mut csv = Vec::new();
	let mut row = Vec::new();
	let mut cell = String::new();
	let mut inside_quote = false;

	let normalized_content = content.replace("\r\n", "\n").replace('\r', "\n");
	let trimmed_content = normalized_content.trim_matches('\n');

	if trimmed_content.is_empty() {
		return Ok(csv);
	}

	let mut iter = trimmed_content.chars().peekable();

	while let Some(character) = iter.next() {
		match character {
			'"' if inside_quote && iter.peek() == Some(&'"') => {
				cell.push('"');
				iter.next(); // we found an escaped quote "" which we have to reduce to one, that's why we consume the second quote
			},
			'"' => inside_quote = !inside_quote,
			',' if !inside_quote => {
				row.push(take(&mut cell));
			},
			'\n' if !inside_quote => {
				row.push(take(&mut cell));
				csv.push(take(&mut row));
			},
			_ => {
				cell.push(character);
			},
		}
	}

	row.push(cell);
	csv.push(row);

	if inside_quote {
		return Err(CsvParseError::UnterminatedQuote);
	}

	Ok(csv)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn row(fields: &[&str]) -> Vec<String> {
		fields.iter().copied().map(String::from).collect()
	}

	#[test]
	fn parse_csv_test_lf_line_endings() {
		assert_eq!(
			parse(String::from("a,b,c\n1,2,3\n4,5,6")),
			Ok(vec![row(&["a", "b", "c"]), row(&["1", "2", "3"]), row(&["4", "5", "6"])])
		);
	}

	#[test]
	fn parse_csv_test_crlf_line_endings() {
		assert_eq!(
			parse(String::from("a,b,c\r\n1,2,3\r\n4,5,6\n")),
			Ok(vec![row(&["a", "b", "c"]), row(&["1", "2", "3"]), row(&["4", "5", "6"])])
		);
	}

	#[test]
	fn parse_csv_test_trailing_blank_lines_ignored() {
		assert_eq!(
			parse(String::from("a,b,c\n1,2,3\n4,5,6\n\n")),
			Ok(vec![row(&["a", "b", "c"]), row(&["1", "2", "3"]), row(&["4", "5", "6"])])
		);
	}

	#[test]
	fn parse_csv_test_quoted_fields() {
		// Covers escaped quotes (""), an embedded newline inside quotes, and empty quoted fields.
		assert_eq!(
			parse(String::from("a,b,c\n\"1\",\"\"\"2\n,\"\"\",3\n4,\"\",6")),
			Ok(vec![
				row(&["a", "b", "c"]),
				row(&["1", "\"2\n,\"", "3"]),
				row(&["4", "", "6"])
			])
		);
	}

	#[test]
	fn parse_csv_test_whitespace_preserved() {
		assert_eq!(parse(String::from("\ta,b,c\n1,2,3  ")), Ok(vec![row(&["\ta", "b", "c"]), row(&["1", "2", "3  "])]));
	}

	#[test]
	fn parse_csv_test_empty_fields() {
		assert_eq!(
			parse(String::from("a,b,c\n,,\n4,5,6")),
			Ok(vec![row(&["a", "b", "c"]), row(&["", "", ""]), row(&["4", "5", "6"])])
		);
	}

	#[test]
	fn parse_csv_test_trailing_comma_is_empty_field() {
		assert_eq!(parse(String::from("a,b,")), Ok(vec![row(&["a", "b", ""])]));
	}

	#[test]
	fn parse_csv_test_trailing_lone_comma_row() {
		assert_eq!(parse(String::from("a,b\n,")), Ok(vec![row(&["a", "b"]), row(&["", ""])]));
	}

	#[test]
	fn parse_csv_test_lone_comma() {
		assert_eq!(parse(String::from(",")), Ok(vec![row(&["", ""])]));
	}

	#[test]
	fn parse_csv_test_lone_empty_quoted_field() {
		assert_eq!(parse(String::from("\"\"")), Ok(vec![row(&[""])]));
	}

	#[test]
	fn parse_csv_test_empty_input() {
		assert_eq!(parse(String::from("")), Ok(vec![]));
	}

	#[test]
	fn parse_csv_test_blank_line_between_rows() {
		// A blank line in the middle parses as a row with a single empty field,
		// which is distinct from the trailing-blank-lines case.
		assert_eq!(
			parse(String::from("a,b,c\n\n4,5,6")),
			Ok(vec![row(&["a", "b", "c"]), row(&[""]), row(&["4", "5", "6"])])
		);
	}

	#[test]
	fn parse_csv_test_unterminated_quote_mid_field() {
		assert_eq!(parse(String::from("a,b,c\n1,2,\"3\n4,5,6")), Err(CsvParseError::UnterminatedQuote));
	}

	#[test]
	fn parse_csv_test_unescaped_quote_inside_quoted_field() {
		assert_eq!(parse(String::from("a,b,c\n\"1\",\"\"2,3\n4,5,6\"")), Err(CsvParseError::UnterminatedQuote));
	}
}
