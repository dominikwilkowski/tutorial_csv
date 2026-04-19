#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CsvParseError {
	InconsistentColumnCount,
	MissingQuote,
}

pub fn parse_csv(content: String) -> Result<Vec<Vec<String>>, CsvParseError> {
	let mut csv = Vec::new();
	let mut row = Vec::new();
	let mut cell = String::new();
	let mut column_count = None;
	let mut inside_quote = false;

	let mut iter = content.chars().peekable();
	while let Some(c) = iter.next() {
		match c {
			'"' => {
				if let Some(next_c) = iter.peek()
					&& *next_c == '"'
				{
					cell.push('"');
					iter.next(); // consume the second quote
				} else {
					inside_quote = !inside_quote;
				}
			},
			',' if !inside_quote => {
				row.push(cell.clone());
				cell.clear();
			},
			'\n' if !inside_quote => {
				if !cell.is_empty() {
					row.push(cell.clone());
					cell.clear();
				}
				csv.push(row.clone());
				if let Some(cols) = column_count
					&& cols != row.len()
				{
					return Err(CsvParseError::InconsistentColumnCount);
				} else {
					column_count = Some(row.len());
				}
				row.clear();
			},
			_ => {
				cell.push(c);
			},
		}
	}

	if !cell.is_empty() {
		row.push(cell.clone());
		if let Some(cols) = column_count
			&& cols != row.len()
		{
			return Err(CsvParseError::InconsistentColumnCount);
		}
		csv.push(row);
	}

	if inside_quote {
		return Err(CsvParseError::MissingQuote);
	}

	Ok(csv)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_csv_normal_trailing_newline() {
		assert_eq!(
			parse_csv(String::from("a,b,c\n1,2,3\n4,5,6\n")),
			Ok(vec![
				vec![String::from("a"), String::from("b"), String::from("c")],
				vec![String::from("1"), String::from("2"), String::from("3")],
				vec![String::from("4"), String::from("5"), String::from("6")]
			])
		);
	}

	#[test]
	fn parse_csv_normal_no_trailing_newline() {
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
	fn parse_csv_quotes() {
		assert_eq!(
			parse_csv(String::from("a,b,c\n\"1\",\"\"2\"\",3\n4,5,6")),
			Ok(vec![
				vec![String::from("a"), String::from("b"), String::from("c")],
				vec![String::from("1"), String::from("\"2\""), String::from("3")],
				vec![String::from("4"), String::from("5"), String::from("6")]
			])
		);
	}

	#[test]
	fn parse_csv_missing_quotes() {
		assert_eq!(parse_csv(String::from("a,b,c\n1,2,\"3\n4,5,6")), Err(CsvParseError::MissingQuote));
		assert_eq!(parse_csv(String::from("a,b,c\n\"1\",\"\"2,3\n4,5,6\"")), Err(CsvParseError::MissingQuote));
	}

	#[test]
	fn parse_csv_inconsistent_columns() {
		assert_eq!(parse_csv(String::from("a,b,c\n1,2,3\n4,5,6,7")), Err(CsvParseError::InconsistentColumnCount));
		assert_eq!(parse_csv(String::from("a,b,c\n1,2,3\n4,5,6,7\n")), Err(CsvParseError::InconsistentColumnCount));
	}
}
