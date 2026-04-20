use std::mem::take;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CsvParseError {
	UnterminatedQuote,
}

pub fn parse_csv(content: String) -> Result<Vec<Vec<String>>, CsvParseError> {
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
				iter.next(); // we found an escaped quote "" which we have to reduce to one so now we consume the second quote
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

	#[test]
	fn parse_csv_test_normal_trailing_newline() {
		assert_eq!(
			parse_csv(String::from("a,b,c\n1,2,3\n4,5,6\n\n")),
			Ok(vec![
				vec![String::from("a"), String::from("b"), String::from("c")],
				vec![String::from("1"), String::from("2"), String::from("3")],
				vec![String::from("4"), String::from("5"), String::from("6")]
			])
		);
	}

	#[test]
	fn parse_csv_test_normal_no_trailing_newline() {
		assert_eq!(
			parse_csv(String::from("a,b,c\n1,2,3\n4,5,6")),
			Ok(vec![
				vec![String::from("a"), String::from("b"), String::from("c")],
				vec![String::from("1"), String::from("2"), String::from("3")],
				vec![String::from("4"), String::from("5"), String::from("6")]
			])
		);
	}

	#[test]
	fn parse_csv_test_normal_carriage_return() {
		assert_eq!(
			parse_csv(String::from("a,b,c\r\n1,2,3\r\n4,5,6\n")),
			Ok(vec![
				vec![String::from("a"), String::from("b"), String::from("c")],
				vec![String::from("1"), String::from("2"), String::from("3")],
				vec![String::from("4"), String::from("5"), String::from("6")]
			])
		);
	}

	#[test]
	fn parse_csv_test_quotes() {
		assert_eq!(
			parse_csv(String::from("a,b,c\n\"1\",\"\"\"2\n,\"\"\",3\n4,\"\",6")),
			Ok(vec![
				vec![String::from("a"), String::from("b"), String::from("c")],
				vec![String::from("1"), String::from("\"2\n,\""), String::from("3")],
				vec![String::from("4"), String::from(""), String::from("6")]
			])
		);
	}

	#[test]
	fn parse_csv_test_whitespace() {
		assert_eq!(
			parse_csv(String::from("\ta,b,c\n1,2,3  ")),
			Ok(vec![
				vec![String::from("\ta"), String::from("b"), String::from("c")],
				vec![String::from("1"), String::from("2"), String::from("3  ")],
			])
		);
	}

	#[test]
	fn parse_csv_test_empty_rows() {
		assert_eq!(
			parse_csv(String::from("a,b,c\n,,\n4,5,6")),
			Ok(vec![
				vec![String::from("a"), String::from("b"), String::from("c")],
				vec![String::from(""), String::from(""), String::from("")],
				vec![String::from("4"), String::from("5"), String::from("6")]
			])
		);
	}

	#[test]
	fn parse_csv_test_trailing_data() {
		assert_eq!(
			parse_csv(String::from("a,b,")),
			Ok(vec![vec![String::from("a"), String::from("b"), String::from("")],])
		);

		assert_eq!(parse_csv(String::from(",")), Ok(vec![vec![String::from(""), String::from("")],]));

		assert_eq!(parse_csv(String::from("\"\"")), Ok(vec![vec![String::from("")]]));

		assert_eq!(
			parse_csv(String::from("a,b\n,")),
			Ok(vec![
				vec![String::from("a"), String::from("b")],
				vec![String::from(""), String::from("")],
			]),
		);
	}

	#[test]
	fn parse_csv_test_malformed_csv() {
		assert_eq!(
			parse_csv(String::from("a,b,c\n\n4,5,6")),
			Ok(vec![
				vec![String::from("a"), String::from("b"), String::from("c")],
				vec![String::from("")],
				vec![String::from("4"), String::from("5"), String::from("6")]
			])
		);

		assert_eq!(parse_csv(String::from("")), Ok(vec![]));

		assert_eq!(parse_csv(String::from("\"\"")), Ok(vec![vec![String::from("")]]));

		assert_eq!(parse_csv(String::from("a,b,")), Ok(vec![vec![String::from("a"), String::from("b"), String::from("")]]));

		assert_eq!(
			parse_csv(String::from("a,\"b\",c")),
			Ok(vec![vec![String::from("a"), String::from("b"), String::from("c")]])
		);
	}

	#[test]
	fn parse_csv_test_missing_quotes() {
		assert_eq!(parse_csv(String::from("a,b,c\n1,2,\"3\n4,5,6")), Err(CsvParseError::UnterminatedQuote));
		assert_eq!(parse_csv(String::from("a,b,c\n\"1\",\"\"2,3\n4,5,6\"")), Err(CsvParseError::UnterminatedQuote));
	}
}
