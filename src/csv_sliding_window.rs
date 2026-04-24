use std::{fs::File, io::Read, mem::take, path::PathBuf, str};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CsvParseError {
	UnableToOpenFile,
	UnterminatedQuote,
	CantReadUtf8,
}

#[derive(Debug, PartialEq, Default)]
struct InputState<'a> {
	chunk: &'a [u8],
	row: Vec<String>,
	cell: String,
	inside_quote: bool,
	last_char_was_cr: bool,
	last_tail: ([u8; 3], usize),
}

#[derive(Debug, PartialEq, Default)]
struct OutputState {
	csv: Vec<Vec<String>>,
	row: Vec<String>,
	cell: String,
	inside_quote: bool,
	last_char_was_cr: bool,
	last_tail: ([u8; 3], usize),
}

const BUFFER_SIZE: usize = 4096;

pub fn parse_csv(csv_file: PathBuf) -> Result<Vec<Vec<String>>, CsvParseError> {
	match File::open(csv_file) {
		Ok(mut content) => {
			let mut buffer = [0_u8; BUFFER_SIZE];
			let mut csv = Vec::new();
			let mut row = Vec::new();
			let mut cell = String::new();
			let mut inside_quote = false;
			let mut last_char_was_cr = false;
			let mut last_tail = ([0; 3], 0);

			while let Ok(bytes_read) = content.read(&mut buffer) {
				if bytes_read == 0 {
					break;
				}

				// `read` may legally return fewer bytes than the buffer holds (short reads near EOF,
				// or on pipes/sockets), so we slice down to what was actually filled.
				let chunk = &buffer[..bytes_read];

				match parse(InputState {
					chunk,
					row,
					cell,
					inside_quote,
					last_char_was_cr,
					last_tail,
				}) {
					Ok(result) => {
						csv.extend(result.csv);
						row = result.row;
						cell = result.cell;
						inside_quote = result.inside_quote;
						last_char_was_cr = result.last_char_was_cr;
						last_tail = result.last_tail;
					},
					Err(error) => {
						return Err(error);
					},
				}
			}

			if inside_quote {
				return Err(CsvParseError::UnterminatedQuote);
			}

			// only add a trailing row if the file didn't end with a newline
			if !cell.is_empty() || !row.is_empty() {
				row.push(cell);
				csv.push(row);
			}

			// since we won't know in the parser function when we've come to the end of the file
			// this is the only time we can remove empty rows at the end
			while csv.pop_if(|row| row.iter().all(String::is_empty)).is_some() {}

			Ok(csv)
		},
		Err(_) => Err(CsvParseError::UnableToOpenFile),
	}
}

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

fn parse(state: InputState) -> Result<OutputState, CsvParseError> {
	let tail_start = utf8_tail_start(state.chunk);
	let (valid_prefix, tail) = state.chunk.split_at(tail_start);

	let mut combine_buffer = [0_u8; BUFFER_SIZE + 3];
	let tail_len = state.last_tail.1;
	let full_content: &[u8] = if tail_len == 0 {
		valid_prefix
	} else {
		combine_buffer[..tail_len].copy_from_slice(&state.last_tail.0[..tail_len]);
		combine_buffer[tail_len..tail_len + valid_prefix.len()].copy_from_slice(valid_prefix);
		&combine_buffer[..tail_len + valid_prefix.len()]
	};

	let text = str::from_utf8(full_content).map_err(|_| CsvParseError::CantReadUtf8)?;
	let mut iter = text.chars().peekable();

	let mut tail_buffer = [0_u8; 3];
	tail_buffer[..tail.len()].copy_from_slice(tail);

	let mut output = OutputState {
		csv: Vec::new(),
		row: state.row,
		cell: state.cell,
		inside_quote: state.inside_quote,
		last_char_was_cr: state.last_char_was_cr,
		last_tail: (tail_buffer, tail.len()),
	};

	while let Some(character) = iter.next() {
		match character {
			'"' if output.inside_quote && iter.peek() == Some(&'"') => {
				output.last_char_was_cr = false;
				output.cell.push('"');
				iter.next(); // we found an escaped quote "" which we have to reduce to one, that's why we consume the second quote
			},
			'"' => {
				output.last_char_was_cr = false;
				output.inside_quote = !output.inside_quote
			},
			',' if !output.inside_quote => {
				output.last_char_was_cr = false;
				output.row.push(take(&mut output.cell));
			},
			'\r' => {
				output.last_char_was_cr = true;
				// normalize `\r\n` and lone `\r` to `\n`
				if iter.peek() == Some(&'\n') {
					iter.next();
				}
				if output.inside_quote {
					output.cell.push('\n');
				} else {
					output.row.push(take(&mut output.cell));
					output.csv.push(take(&mut output.row));
				}
			},
			'\n' if output.last_char_was_cr => {
				// We've hit a case where the sliding window split an old school windows `\r\n` and so
				// we need to ignore the `\n` to not count them twice
				output.last_char_was_cr = false;
			},
			'\n' if !output.inside_quote => {
				output.last_char_was_cr = false;
				output.row.push(take(&mut output.cell));
				output.csv.push(take(&mut output.row));
			},
			_ => {
				output.last_char_was_cr = false;
				output.cell.push(character);
			},
		}
	}

	Ok(output)
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
			parse(InputState {
				chunk: String::from("a,b,c\n1,2,3\n4,5,6").as_bytes(),
				..Default::default()
			}),
			Ok(OutputState {
				csv: vec![row(&["a", "b", "c"]), row(&["1", "2", "3"])],
				row: row(&["4", "5"]),
				cell: String::from("6"),
				..Default::default()
			})
		);
	}

	#[test]
	fn parse_csv_test_crlf_line_endings() {
		assert_eq!(
			parse(InputState {
				chunk: String::from("a,b,c\r\n1,2,3\r\n4,5,6\n").as_bytes(),
				..Default::default()
			}),
			Ok(OutputState {
				csv: vec![row(&["a", "b", "c"]), row(&["1", "2", "3"]), row(&["4", "5", "6"])],
				..Default::default()
			})
		);
	}

	#[test]
	fn parse_csv_test_trailing_blank_lines_included() {
		assert_eq!(
			parse(InputState {
				chunk: String::from("a,b,c\n1,2,3\n4,5,6\n\n").as_bytes(),
				..Default::default()
			}),
			Ok(OutputState {
				csv: vec![
					row(&["a", "b", "c"]),
					row(&["1", "2", "3"]),
					row(&["4", "5", "6"]),
					row(&[""])
				],
				..Default::default()
			})
		);
	}

	#[test]
	fn parse_csv_test_quoted_fields() {
		// Covers escaped quotes (""), an embedded newline inside quotes, and empty quoted fields.
		assert_eq!(
			parse(InputState {
				chunk: String::from("a,b,c\n\"1\",\"\"\"2\n,\"\"\",3\n4,\"\",6").as_bytes(),
				..Default::default()
			}),
			Ok(OutputState {
				csv: vec![row(&["a", "b", "c"]), row(&["1", "\"2\n,\"", "3"]),],
				row: row(&["4", ""]),
				cell: String::from("6"),
				..Default::default()
			})
		);
	}

	#[test]
	fn parse_csv_test_whitespace_preserved() {
		assert_eq!(
			parse(InputState {
				chunk: String::from("\ta,b,c\n1,2,3  ").as_bytes(),
				..Default::default()
			}),
			Ok(OutputState {
				csv: vec![row(&["\ta", "b", "c"])],
				row: row(&["1", "2"]),
				cell: String::from("3  "),
				..Default::default()
			})
		);
	}

	#[test]
	fn parse_csv_test_empty_fields() {
		assert_eq!(
			parse(InputState {
				chunk: String::from("a,b,c\n,,\n4,5,6").as_bytes(),
				..Default::default()
			}),
			Ok(OutputState {
				csv: vec![row(&["a", "b", "c"]), row(&["", "", ""])],
				row: row(&["4", "5"]),
				cell: String::from("6"),
				..Default::default()
			})
		);
	}

	#[test]
	fn parse_csv_test_trailing_comma_is_empty_field() {
		assert_eq!(
			parse(InputState {
				chunk: String::from("a,b,").as_bytes(),
				..Default::default()
			}),
			Ok(OutputState {
				csv: Vec::new(),
				row: row(&["a", "b"]),
				..Default::default()
			})
		);
	}

	#[test]
	fn parse_csv_test_trailing_lone_comma_row() {
		assert_eq!(
			parse(InputState {
				chunk: String::from("a,b\n,").as_bytes(),
				..Default::default()
			}),
			Ok(OutputState {
				csv: vec![row(&["a", "b"])],
				row: row(&[""]),
				..Default::default()
			})
		);
	}

	#[test]
	fn parse_csv_test_lone_comma() {
		assert_eq!(
			parse(InputState {
				chunk: String::from(",").as_bytes(),
				..Default::default()
			}),
			Ok(OutputState {
				csv: Vec::new(),
				row: row(&[""]),
				..Default::default()
			})
		);
	}

	#[test]
	fn parse_csv_test_lone_empty_quoted_field() {
		assert_eq!(
			parse(InputState {
				chunk: String::from("\"\"").as_bytes(),
				..Default::default()
			}),
			Ok(OutputState {
				csv: Vec::new(),
				..Default::default()
			})
		);
	}

	#[test]
	fn parse_csv_test_empty_input() {
		assert_eq!(
			parse(InputState {
				chunk: String::from("").as_bytes(),
				..Default::default()
			}),
			Ok(OutputState {
				csv: Vec::new(),
				..Default::default()
			})
		);
	}

	#[test]
	fn parse_csv_test_blank_line_between_rows() {
		// A blank line in the middle parses as a row with a single empty field,
		// which is distinct from the trailing-blank-lines case.
		assert_eq!(
			parse(InputState {
				chunk: String::from("a,b,c\n\n4,5,6").as_bytes(),
				..Default::default()
			}),
			Ok(OutputState {
				csv: vec![row(&["a", "b", "c"]), row(&[""])],
				row: row(&["4", "5"]),
				cell: String::from("6"),
				..Default::default()
			})
		);
	}

	#[test]
	fn parse_csv_test_unterminated_quote_mid_field() {
		assert_eq!(
			parse(InputState {
				chunk: String::from("a,b,c\n1,2,\"3\n4,5,6").as_bytes(),
				..Default::default()
			}),
			Ok(OutputState {
				csv: vec![row(&["a", "b", "c"])],
				row: row(&["1", "2"]),
				cell: String::from("3\n4,5,6"),
				inside_quote: true,
				..Default::default()
			})
		);
	}

	#[test]
	fn parse_csv_test_unescaped_quote_inside_quoted_field() {
		assert_eq!(
			parse(InputState {
				chunk: String::from("a,b,c\n\"1\",\"\"2,3\n4,5,6\"").as_bytes(),
				..Default::default()
			}),
			Ok(OutputState {
				csv: vec![row(&["a", "b", "c"]), row(&["1", "2", "3"])],
				row: row(&["4", "5"]),
				cell: String::from("6"),
				inside_quote: true,
				..Default::default()
			})
		);
	}

	#[test]
	fn parse_csv_test_break_new_line_sequence() {
		let whole_content = "a,b,c\r\n1,2,3".as_bytes();
		let (content_left, content_right): (&[u8], &[u8]) = whole_content.split_at(whole_content.len() / 2);

		assert_eq!(
			parse(InputState {
				chunk: content_left,
				..Default::default()
			}),
			Ok(OutputState {
				csv: vec![row(&["a", "b", "c"])],
				last_char_was_cr: true,
				..Default::default()
			})
		);

		assert_eq!(
			parse(InputState {
				chunk: content_right,
				last_char_was_cr: true,
				..Default::default()
			}),
			Ok(OutputState {
				csv: Vec::new(),
				row: row(&["1", "2"]),
				cell: String::from("3"),
				..Default::default()
			})
		);
	}

	#[test]
	fn parse_csv_test_break_4_byte_utf8_sequence() {
		let whole_content = "a,🧑🏿‍💻,c".as_bytes();
		let (content_left, content_right): (&[u8], &[u8]) = whole_content.split_at(whole_content.len() / 2);

		assert_eq!(
			parse(InputState {
				chunk: content_left,
				..Default::default()
			}),
			Ok(OutputState {
				csv: Vec::new(),
				row: row(&["a"]),
				cell: String::from("🧑"),
				last_tail: ([240, 159, 143], 3),
				..Default::default()
			})
		);

		assert_eq!(
			parse(InputState {
				chunk: content_right,
				row: row(&["a"]),
				cell: String::from("🧑"),
				last_tail: ([240, 159, 143], 3),
				..Default::default()
			}),
			Ok(OutputState {
				csv: Vec::new(),
				row: row(&["a", "🧑🏿‍💻"]),
				cell: String::from("c"),
				..Default::default()
			})
		);
	}

	#[test]
	fn parse_csv_test_break_3_byte_utf8_sequence() {
		let whole_content = "a,€,c".as_bytes();
		let (content_left, content_right): (&[u8], &[u8]) = whole_content.split_at(whole_content.len() / 2);

		assert_eq!(
			parse(InputState {
				chunk: content_left,
				..Default::default()
			}),
			Ok(OutputState {
				csv: Vec::new(),
				row: row(&["a"]),
				cell: String::from(""),
				last_tail: ([226, 0, 0], 1),
				..Default::default()
			})
		);

		assert_eq!(
			parse(InputState {
				chunk: content_right,
				row: row(&["a"]),
				cell: String::from(""),
				last_tail: ([226, 0, 0], 1),
				..Default::default()
			}),
			Ok(OutputState {
				csv: Vec::new(),
				row: row(&["a", "€"]),
				cell: String::from("c"),
				..Default::default()
			})
		);
	}

	#[test]
	fn parse_csv_test_break_2_byte_utf8_sequence() {
		let whole_content = "a,é,c".as_bytes();
		let (content_left, content_right): (&[u8], &[u8]) = whole_content.split_at(whole_content.len() / 2);

		assert_eq!(
			parse(InputState {
				chunk: content_left,
				..Default::default()
			}),
			Ok(OutputState {
				csv: Vec::new(),
				row: row(&["a"]),
				cell: String::from(""),
				last_tail: ([195, 0, 0], 1),
				..Default::default()
			})
		);

		assert_eq!(
			parse(InputState {
				chunk: content_right,
				row: row(&["a"]),
				cell: String::from(""),
				last_tail: ([195, 0, 0], 1),
				..Default::default()
			}),
			Ok(OutputState {
				csv: Vec::new(),
				row: row(&["a", "é"]),
				cell: String::from("c"),
				..Default::default()
			})
		);
	}

	#[test]
	fn parse_csv_test_split_on_1_byte_utf8_boundary() {
		let whole_content = "a,b,c".as_bytes();
		let (content_left, content_right): (&[u8], &[u8]) = whole_content.split_at(whole_content.len() / 2);

		assert_eq!(
			parse(InputState {
				chunk: content_left,
				..Default::default()
			}),
			Ok(OutputState {
				csv: Vec::new(),
				row: row(&["a"]),
				cell: String::from(""),
				..Default::default()
			})
		);

		assert_eq!(
			parse(InputState {
				chunk: content_right,
				row: row(&["a"]),
				cell: String::from(""),
				..Default::default()
			}),
			Ok(OutputState {
				csv: Vec::new(),
				row: row(&["a", "b"]),
				cell: String::from("c"),
				..Default::default()
			})
		);
	}

	#[test]
	fn utf8_tail_start_test_edgecases() {
		assert_eq!(utf8_tail_start(b""), 0);
		assert_eq!(utf8_tail_start(b"a"), 1);
		assert_eq!(utf8_tail_start("é".as_bytes()), "é".len());
		assert_eq!(utf8_tail_start("€".as_bytes()), "€".len());
		assert_eq!(utf8_tail_start("💩".as_bytes()), "💩".len());
		assert_eq!(utf8_tail_start("🧑🏿‍💻".as_bytes()), "🧑🏿‍💻".len());

		assert_eq!(utf8_tail_start(&[0xC3]), 0); // start of 2-byte char
		assert_eq!(utf8_tail_start(&[0xE2]), 0); // start of 3-byte char
		assert_eq!(utf8_tail_start(&[0xE2, 0x82]), 0); // 2/3 bytes of 3-byte char
		assert_eq!(utf8_tail_start(&[0xF0]), 0); // start of 4-byte char
		assert_eq!(utf8_tail_start(&[0xF0, 0x9F]), 0); // 2/4 bytes
		assert_eq!(utf8_tail_start(&[0xF0, 0x9F, 0x92]), 0); // 3/4 bytes

		let s = "aéüß€🧑🏿‍💻💩";
		for i in 0..=s.len() {
			let chunk = &s.as_bytes()[..i];
			let split = utf8_tail_start(chunk);
			assert!(std::str::from_utf8(&chunk[..split]).is_ok());
		}
	}
}
