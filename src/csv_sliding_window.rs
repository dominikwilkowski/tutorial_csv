use std::{fs::File, io::Read, mem::take, path::PathBuf, str};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CsvParseError {
	UnableToOpenFile,
	UnterminatedQuote,
	CantReadUtf8,
}

const BUFFER_SIZE: usize = 65536;

#[derive(Debug, PartialEq, Default)]
struct CsvParser {
	row: Vec<String>,
	cell: Vec<u8>,
	has_structure: bool,
	pending_empty_rows: usize,
	inside_quote: bool,
	last_char_was_cr: bool,
	last_char_was_quote: bool,
}

impl CsvParser {
	fn parse_into(&mut self, chunk: &str, csv: &mut Vec<Vec<String>>) {
		let mut iter = chunk.bytes().peekable();

		while let Some(byte) = iter.next() {
			if !self.inside_quote && self.pending_empty_rows > 0 {
				match byte {
					b'\r' | b'\n' => {
						// still in a run of blank lines; they will be handled in the next match below
					},
					_ => {
						for _ in 0..self.pending_empty_rows {
							csv.push(vec![String::new()]);
						}
						self.pending_empty_rows = 0;
					},
				}
			}

			match byte {
				b'"' if self.last_char_was_quote => {
					self.has_structure = true;
					self.last_char_was_cr = false;
					self.last_char_was_quote = false;
					// previous chunk ended with a `"` that we processed as a close, but seeing another `"`
					// here means that close was actually the first half of a `""` escape split across chunks;
					// undo the close and emit the literal `"` we should have pushed back then
					self.inside_quote = true;
					self.cell.push(b'"');
				},
				b'"' if self.inside_quote && iter.peek() == Some(&b'"') => {
					self.has_structure = true;
					self.last_char_was_cr = false;
					self.last_char_was_quote = false;
					self.cell.push(b'"');
					iter.next(); // we found an escaped quote "" which we have to reduce to one, that's why we consume the second quote
				},
				b'"' => {
					self.has_structure = true;
					self.last_char_was_cr = false;
					// only ambiguous when closing at end of iter - opens and mid-chunk closes are unambiguous
					self.last_char_was_quote = self.inside_quote && iter.peek().is_none();
					self.inside_quote = !self.inside_quote;
				},
				b',' if !self.inside_quote => {
					self.has_structure = true;
					self.last_char_was_cr = false;
					self.last_char_was_quote = false;
					self.row.push(String::from_utf8(self.cell.clone()).expect("validated upstream"));
					self.cell.clear();
				},
				b'\r' => {
					self.last_char_was_quote = false;

					let had_lf_in_same_chunk = iter.peek() == Some(&b'\n');
					if had_lf_in_same_chunk {
						// normalize `\r\n` and lone `\r` to `\n`
						iter.next();
					}

					// only leave this set when the LF might arrive in the next chunk
					self.last_char_was_cr = !had_lf_in_same_chunk;

					if self.inside_quote {
						self.cell.push(b'\n');
					} else {
						if self.has_structure || !self.row.is_empty() || !self.cell.is_empty() {
							self.row.push(String::from_utf8(self.cell.clone()).expect("validated upstream"));
							self.cell.clear();
							csv.push(take(&mut self.row));
						} else {
							self.pending_empty_rows += 1;
						}
						self.has_structure = false;
					}
				},
				b'\n' if self.last_char_was_cr => {
					// We've hit a case where the sliding window split an old school windows `\r\n` and so
					// we need to ignore the `\n` to not count them twice
					self.last_char_was_cr = false;
					self.last_char_was_quote = false;
				},
				b'\n' if !self.inside_quote => {
					self.last_char_was_cr = false;
					self.last_char_was_quote = false;

					if self.has_structure || !self.row.is_empty() || !self.cell.is_empty() {
						self.row.push(String::from_utf8(self.cell.clone()).expect("validated upstream"));
						self.cell.clear();
						csv.push(take(&mut self.row));
					} else {
						self.pending_empty_rows += 1;
					}
					self.has_structure = false;
				},
				_ => {
					self.has_structure = true;
					self.last_char_was_cr = false;
					self.last_char_was_quote = false;
					self.cell.push(byte);
				},
			}
		}
	}
}

#[derive(Debug)]
pub struct Csv {
	parser: CsvParser,
	file: File,
	buffer: [u8; BUFFER_SIZE],
	tail_len: usize,
	pending: Vec<Vec<String>>,
	pending_index: usize,
	finished: bool,
}

impl Csv {
	/// Returns the index where an possibly truncated final codepoint begins or `bytes.len()`
	/// if the slice ends on a clean codepoint boundary.
	fn utf8_tail_start(bytes: &[u8]) -> usize {
		// the tail can't exceed 3 bytes: a 4-byte codepoint is UTF-8's longest,
		// so any incomplete tail is 1-3 bytes of a 2/3/4-byte codepoint
		for offset in 1..=3.min(bytes.len()) {
			let byte = bytes[bytes.len() - offset];

			// continuation bytes (10xxxxxx) aren't the start of anything; keep walking
			// https://www.rfc-editor.org/rfc/rfc3629#section-3
			if byte & 0b1100_0000 == 0b1000_0000 {
				continue;
			}

			// leading byte found; the count of leading 1-bits gives the codepoint's length
			// (0 leading ones = ASCII, 2/3/4 = multi-byte, anything else is malformed)
			let expected = match byte.leading_ones() {
				0 => 1,
				count @ 2..=4 => count as usize,
				_ => return bytes.len(),
			};

			return if offset < expected {
				bytes.len() - offset
			} else {
				bytes.len()
			};
		}
		bytes.len()
	}

	pub fn parse_file(path: PathBuf) -> Result<Self, CsvParseError> {
		Ok(Self {
			parser: CsvParser::default(),
			file: File::open(path).map_err(|_| CsvParseError::UnableToOpenFile)?,
			buffer: [0; BUFFER_SIZE],
			tail_len: 0,
			pending: Vec::new(),
			pending_index: 0,
			finished: false,
		})
	}

	fn next_pending(&mut self) -> Option<Vec<String>> {
		if self.pending_index >= self.pending.len() {
			return None;
		}

		let row = take(&mut self.pending[self.pending_index]);
		self.pending_index += 1;

		if self.pending_index == self.pending.len() {
			self.pending.clear();
			self.pending_index = 0;
		}

		Some(row)
	}

	fn finish(&mut self) -> Option<Result<Vec<String>, CsvParseError>> {
		self.finished = true;

		if self.parser.inside_quote {
			return Some(Err(CsvParseError::UnterminatedQuote));
		}

		if self.parser.has_structure || !self.parser.cell.is_empty() || !self.parser.row.is_empty() {
			self.parser.row.push(String::from_utf8(take(&mut self.parser.cell)).expect("validated upstream"));
			return Some(Ok(take(&mut self.parser.row)));
		}

		None
	}
}

impl Iterator for Csv {
	type Item = Result<Vec<String>, CsvParseError>;

	fn next(&mut self) -> Option<Self::Item> {
		if let Some(row) = self.next_pending() {
			return Some(Ok(row));
		}

		if self.finished {
			return None;
		}

		loop {
			let bytes_read = match self.file.read(&mut self.buffer[self.tail_len..]) {
				Ok(0) => {
					if self.tail_len != 0 {
						self.finished = true;
						return Some(Err(CsvParseError::CantReadUtf8));
					}
					return self.finish();
				},
				Ok(n) => n,
				Err(_) => {
					self.finished = true;
					return Some(Err(CsvParseError::UnableToOpenFile));
				},
			};

			let total_bytes = self.tail_len + bytes_read;
			let tail_start = Self::utf8_tail_start(&self.buffer[..total_bytes]);

			let text = match str::from_utf8(&self.buffer[..tail_start]) {
				Ok(text) => text,
				Err(_) => {
					self.finished = true;
					return Some(Err(CsvParseError::CantReadUtf8));
				},
			};

			self.pending.clear();
			self.pending_index = 0;
			self.parser.parse_into(text, &mut self.pending);

			// shift any incomplete codepoint to the front so the next read appends right after it
			self.buffer.copy_within(tail_start..total_bytes, 0);
			self.tail_len = total_bytes - tail_start;

			if let Some(row) = self.next_pending() {
				return Some(Ok(row));
			}
			// this chunk produced no complete rows so keep reading
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::{
		env,
		fs::{remove_file, write},
	};

	fn row(fields: &[&str]) -> Vec<String> {
		fields.iter().copied().map(String::from).collect()
	}

	#[test]
	fn parse_into_test_lf_line_endings() {
		let mut state = CsvParser::default();
		let mut result_row = Vec::new();
		state.parse_into("a,b,c\n1,2,3\n4,5,6", &mut result_row);

		assert_eq!(result_row, vec![row(&["a", "b", "c"]), row(&["1", "2", "3"])]);
		assert_eq!(
			state,
			CsvParser {
				row: row(&["4", "5"]),
				cell: Vec::from(b"6"),
				has_structure: true,
				..Default::default()
			}
		);
	}

	#[test]
	fn parse_into_test_crlf_line_endings() {
		let mut state = CsvParser::default();
		let mut result_row = Vec::new();
		state.parse_into("a,b,c\r\n1,2,3\r\n4,5,6\n", &mut result_row);

		assert_eq!(result_row, vec![row(&["a", "b", "c"]), row(&["1", "2", "3"]), row(&["4", "5", "6"])]);
		assert_eq!(state, CsvParser::default());
	}

	#[test]
	fn parse_into_test_crlf_followed_by_lf_blank_line() {
		let mut state = CsvParser::default();
		let mut result_row = Vec::new();
		state.parse_into("a\r\n\nb", &mut result_row);

		assert_eq!(result_row, vec![row(&["a"]), row(&[""])]);
		assert_eq!(
			state,
			CsvParser {
				cell: Vec::from(b"b"),
				has_structure: true,
				..Default::default()
			}
		);
	}

	#[test]
	fn parse_into_test_trailing_blank_lines_included() {
		let mut state = CsvParser::default();
		let mut result_row = Vec::new();
		state.parse_into("a,b,c\n1,2,3\n4,5,6\n\n", &mut result_row);

		assert_eq!(result_row, vec![row(&["a", "b", "c"]), row(&["1", "2", "3"]), row(&["4", "5", "6"]),]);
		assert_eq!(
			state,
			CsvParser {
				pending_empty_rows: 1,
				..Default::default()
			}
		);
	}

	#[test]
	fn parse_into_test_quoted_fields() {
		// Covers escaped quotes (""), an embedded newline inside quotes, and empty quoted fields.
		let mut state = CsvParser::default();
		let mut result_row = Vec::new();
		state.parse_into("a,b,c\n\"1\",\"\"\"2\n,\"\"\",3\n4,\"\",6", &mut result_row);

		assert_eq!(result_row, vec![row(&["a", "b", "c"]), row(&["1", "\"2\n,\"", "3"]),]);
		assert_eq!(
			state,
			CsvParser {
				row: row(&["4", ""]),
				cell: Vec::from(b"6"),
				has_structure: true,
				..Default::default()
			}
		);
	}

	#[test]
	fn parse_into_test_whitespace_preserved() {
		let mut state = CsvParser::default();
		let mut result_row = Vec::new();
		state.parse_into("\ta,b,c\n1,2,3  ", &mut result_row);

		assert_eq!(result_row, vec![row(&["\ta", "b", "c"])]);
		assert_eq!(
			state,
			CsvParser {
				row: row(&["1", "2"]),
				cell: Vec::from(b"3  "),
				has_structure: true,
				..Default::default()
			}
		);
	}

	#[test]
	fn parse_into_test_empty_fields() {
		let mut state = CsvParser::default();
		let mut result_row = Vec::new();
		state.parse_into("a,b,c\n,,\n4,5,6", &mut result_row);

		assert_eq!(result_row, vec![row(&["a", "b", "c"]), row(&["", "", ""])]);
		assert_eq!(
			state,
			CsvParser {
				row: row(&["4", "5"]),
				cell: Vec::from(b"6"),
				has_structure: true,
				..Default::default()
			}
		);
	}

	#[test]
	fn parse_into_test_trailing_comma_is_empty_field() {
		let mut state = CsvParser::default();
		let mut result_row = Vec::new();
		state.parse_into("a,b,", &mut result_row);

		assert!(result_row.is_empty());
		assert_eq!(
			state,
			CsvParser {
				row: row(&["a", "b"]),
				has_structure: true,
				..Default::default()
			}
		);
	}

	#[test]
	fn parse_into_test_trailing_lone_comma_row() {
		let mut state = CsvParser::default();
		let mut result_row = Vec::new();
		state.parse_into("a,b\n,", &mut result_row);

		assert_eq!(result_row, vec![row(&["a", "b"])]);
		assert_eq!(
			state,
			CsvParser {
				row: row(&[""]),
				has_structure: true,
				..Default::default()
			}
		);
	}

	#[test]
	fn parse_into_test_lone_comma() {
		let mut state = CsvParser::default();
		let mut result_row = Vec::new();
		state.parse_into(",", &mut result_row);

		assert!(result_row.is_empty());
		assert_eq!(
			state,
			CsvParser {
				row: row(&[""]),
				has_structure: true,
				..Default::default()
			}
		);
	}

	#[test]
	fn parse_into_test_lone_empty_quoted_field() {
		let mut state = CsvParser::default();
		let mut result_row = Vec::new();
		state.parse_into("\"\"", &mut result_row);

		assert!(result_row.is_empty());
		assert_eq!(
			state,
			CsvParser {
				last_char_was_quote: true,
				has_structure: true,
				..Default::default()
			}
		);
	}

	#[test]
	fn parse_into_test_empty_input() {
		let mut state = CsvParser::default();
		let mut result_row = Vec::new();
		state.parse_into("", &mut result_row);

		assert!(result_row.is_empty());
		assert_eq!(state, CsvParser::default());
	}

	#[test]
	fn parse_into_test_blank_line_between_rows() {
		// A blank line in the middle parses as a row with a single empty field,
		// which is distinct from the trailing-blank-lines case.
		let mut state = CsvParser::default();
		let mut result_row = Vec::new();
		state.parse_into("a,b,c\n\n4,5,6", &mut result_row);

		assert_eq!(result_row, vec![row(&["a", "b", "c"]), row(&[""])]);
		assert_eq!(
			state,
			CsvParser {
				row: row(&["4", "5"]),
				cell: Vec::from(b"6"),
				has_structure: true,
				..Default::default()
			}
		);
	}

	#[test]
	fn parse_into_test_unterminated_quote_mid_field() {
		let mut state = CsvParser::default();
		let mut result_row = Vec::new();
		state.parse_into("a,b,c\n1,2,\"3\n4,5,6", &mut result_row);

		assert_eq!(result_row, vec![row(&["a", "b", "c"])]);
		assert_eq!(
			state,
			CsvParser {
				row: row(&["1", "2"]),
				cell: Vec::from(b"3\n4,5,6"),
				inside_quote: true,
				has_structure: true,
				..Default::default()
			}
		);
	}

	#[test]
	fn parse_into_test_unescaped_quote_inside_quoted_field() {
		let mut state = CsvParser::default();
		let mut result_row = Vec::new();
		state.parse_into("a,b,c\n\"1\",\"\"2,3\n4,5,6\"", &mut result_row);

		assert_eq!(result_row, vec![row(&["a", "b", "c"]), row(&["1", "2", "3"])]);
		assert_eq!(
			state,
			CsvParser {
				row: row(&["4", "5"]),
				cell: Vec::from(b"6"),
				inside_quote: true,
				has_structure: true,
				..Default::default()
			}
		);
	}

	#[test]
	fn parse_into_test_break_new_line_sequence() {
		let whole_content = "a,b,c\r\n1,2,3";
		let (content_left, content_right) = whole_content.split_at(whole_content.len() / 2);

		let mut state = CsvParser::default();
		let mut result_row = Vec::new();
		state.parse_into(content_left, &mut result_row);

		assert_eq!(result_row, vec![row(&["a", "b", "c"])]);
		assert_eq!(
			state,
			CsvParser {
				last_char_was_cr: true,
				..Default::default()
			}
		);

		result_row.clear();
		state.parse_into(content_right, &mut result_row);

		assert!(result_row.is_empty());
		assert_eq!(
			state,
			CsvParser {
				row: row(&["1", "2"]),
				cell: Vec::from(b"3"),
				has_structure: true,
				..Default::default()
			}
		);
	}

	// #[test]
	// fn parse_into_test_break_4_byte_utf8_sequence() {
	// 	let whole_content = "a,🧑🏿‍💻,c";
	// 	let (content_left, content_right) = whole_content.split_at(whole_content.len() / 2);

	// 	let mut state = CsvParser::default();
	// 	let mut result_row = Vec::new();
	// 	state.parse_into(content_left, &mut result_row);

	// 	assert!(result_row.is_empty());
	// 	assert_eq!(
	// 		state,
	// 		CsvParser {
	// 			row: row(&["a"]),
	// 			cell: Vec::from(b"🧑"),
	// 			has_structure: true,
	// 			..Default::default()
	// 		}
	// 	);

	// 	state.parse_into(content_right, &mut result_row);

	// 	assert!(result_row.is_empty());
	// 	assert_eq!(
	// 		state,
	// 		CsvParser {
	// 			row: row(&["a", "🧑🏿‍💻"]),
	// 			cell: Vec::from(b"c"),
	// 			has_structure: true,
	// 			..Default::default()
	// 		}
	// 	);
	// }

	// #[test]
	// fn parse_into_test_break_3_byte_utf8_sequence() {
	// 	let whole_content = "a,€,c";
	// 	let (content_left, content_right) = whole_content.split_at(whole_content.len() / 2);

	// 	let mut state = CsvParser::default();
	// 	let mut result_row = Vec::new();
	// 	state.parse_into(content_left, &mut result_row);

	// 	assert!(result_row.is_empty());
	// 	assert_eq!(
	// 		state,
	// 		CsvParser {
	// 			row: row(&["a"]),
	// 			cell: Vec::from(b""),
	// 			has_structure: true,
	// 			..Default::default()
	// 		}
	// 	);

	// 	state.parse_into(content_right, &mut result_row);

	// 	assert!(result_row.is_empty());
	// 	assert_eq!(
	// 		state,
	// 		CsvParser {
	// 			row: row(&["a", "€"]),
	// 			cell: Vec::from(b"c"),
	// 			has_structure: true,
	// 			..Default::default()
	// 		}
	// 	);
	// }

	// #[test]
	// fn parse_into_test_break_2_byte_utf8_sequence() {
	// 	let whole_content = "a,é,c";
	// 	let (content_left, content_right) = whole_content.split_at(whole_content.len() / 2);

	// 	let mut state = CsvParser::default();
	// 	let mut result_row = Vec::new();
	// 	state.parse_into(content_left, &mut result_row);

	// 	assert!(result_row.is_empty());
	// 	assert_eq!(
	// 		state,
	// 		CsvParser {
	// 			row: row(&["a"]),
	// 			cell: Vec::from(b""),
	// 			has_structure: true,
	// 			..Default::default()
	// 		}
	// 	);

	// 	state.parse_into(content_right, &mut result_row);

	// 	assert!(result_row.is_empty());
	// 	assert_eq!(
	// 		state,
	// 		CsvParser {
	// 			row: row(&["a", "é"]),
	// 			cell: Vec::from(b"c"),
	// 			has_structure: true,
	// 			..Default::default()
	// 		}
	// 	);
	// }

	#[test]
	fn parse_into_test_split_on_1_byte_utf8_boundary() {
		let whole_content = "a,b,c";
		let (content_left, content_right) = whole_content.split_at(whole_content.len() / 2);

		let mut state = CsvParser::default();
		let mut result_row = Vec::new();
		state.parse_into(content_left, &mut result_row);

		assert!(result_row.is_empty());
		assert_eq!(
			state,
			CsvParser {
				row: row(&["a"]),
				cell: Vec::from(b""),
				has_structure: true,
				..Default::default()
			}
		);

		state.parse_into(content_right, &mut result_row);

		assert!(result_row.is_empty());
		assert_eq!(
			state,
			CsvParser {
				row: row(&["a", "b"]),
				cell: Vec::from(b"c"),
				has_structure: true,
				..Default::default()
			}
		);
	}

	#[test]
	fn parse_into_test_split_escaped_quote_across_chunks() {
		let whole_content = "a,\"x\"\"y\",c";

		// Split exactly between the two quotes of the escaped quote sequence `""`.
		// Left chunk ends with the first `"`, right chunk starts with the second `"`.
		let (content_left, content_right) = whole_content.split_at(5);

		let mut state = CsvParser::default();
		let mut result_row = Vec::new();
		state.parse_into(content_left, &mut result_row);

		assert!(result_row.is_empty());
		assert_eq!(
			state,
			CsvParser {
				row: row(&["a"]),
				cell: Vec::from(b"x"),
				last_char_was_quote: true,
				has_structure: true,
				..Default::default()
			}
		);

		state.parse_into(content_right, &mut result_row);

		assert!(result_row.is_empty());
		assert_eq!(
			state,
			CsvParser {
				row: row(&["a", "x\"y"]),
				cell: Vec::from(b"c"),
				has_structure: true,
				..Default::default()
			}
		);
	}

	#[test]
	fn utf8_tail_start_test_edgecases() {
		assert_eq!(Csv::utf8_tail_start(b""), 0);
		assert_eq!(Csv::utf8_tail_start(b"a"), 1);
		assert_eq!(Csv::utf8_tail_start("é".as_bytes()), "é".len());
		assert_eq!(Csv::utf8_tail_start("€".as_bytes()), "€".len());
		assert_eq!(Csv::utf8_tail_start("💩".as_bytes()), "💩".len());
		assert_eq!(Csv::utf8_tail_start("🧑🏿‍💻".as_bytes()), "🧑🏿‍💻".len());

		assert_eq!(Csv::utf8_tail_start(&[0xC3]), 0); // start of 2-byte char
		assert_eq!(Csv::utf8_tail_start(&[0xE2]), 0); // start of 3-byte char
		assert_eq!(Csv::utf8_tail_start(&[0xE2, 0x82]), 0); // 2/3 bytes of 3-byte char
		assert_eq!(Csv::utf8_tail_start(&[0xF0]), 0); // start of 4-byte char
		assert_eq!(Csv::utf8_tail_start(&[0xF0, 0x9F]), 0); // 2/4 bytes
		assert_eq!(Csv::utf8_tail_start(&[0xF0, 0x9F, 0x92]), 0); // 3/4 bytes

		let s = "aéüß€🧑🏿‍💻💩";
		for i in 0..=s.len() {
			let chunk = &s.as_bytes()[..i];
			let split = Csv::utf8_tail_start(chunk);
			assert!(std::str::from_utf8(&chunk[..split]).is_ok());
		}
	}

	#[test]
	fn parse_file_test_keeps_final_record_with_only_empty_fields() {
		let path = env::temp_dir().join(format!("csv_test_1.csv"));
		// One real CSV record with two empty fields.
		// This is not just a trailing blank line.
		write(&path, b",\n").unwrap();

		let result = Csv::parse_file(path.clone()).and_then(|csv| csv.collect::<Result<Vec<_>, _>>());
		let _ = remove_file(&path);
		assert_eq!(result, Ok(vec![row(&["", ""])]));
	}

	#[test]
	fn parse_file_test_keeps_final_record_with_quoted_empty_field() {
		let path = env::temp_dir().join(format!("csv_test_2.csv"));
		write(&path, b"\"\"\n").unwrap();

		let result = Csv::parse_file(path.clone()).and_then(|csv| csv.collect::<Result<Vec<_>, _>>());
		let _ = remove_file(&path);
		assert_eq!(result, Ok(vec![row(&[""])]));
	}

	#[test]
	fn parse_file_test_ignores_trailing_blank_lines() {
		let path = env::temp_dir().join(format!("csv_test_3.csv"));
		write(&path, b"a,b,c\n\n").unwrap();

		let result = Csv::parse_file(path.clone()).and_then(|csv| csv.collect::<Result<Vec<_>, _>>());
		let _ = remove_file(&path);
		assert_eq!(result, Ok(vec![row(&["a", "b", "c"])]));
	}

	#[test]
	fn parse_file_test_keeps_single_quoted_empty_field() {
		let path = env::temp_dir().join(format!("csv_test_4.csv"));
		write(&path, b"\"\"").unwrap();

		let result = Csv::parse_file(path.clone()).and_then(|csv| csv.collect::<Result<Vec<_>, _>>());
		let _ = remove_file(&path);
		assert_eq!(result, Ok(vec![row(&[""])]));
	}
}
