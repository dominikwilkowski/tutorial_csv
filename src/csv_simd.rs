use std::simd::{Simd, cmp::SimdPartialEq as _};

const LANE_COUNT: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Delimiter {
	Quote,
	Comma,
	Newline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DelimiterHit {
	position: usize,
	kind: Delimiter,
}

fn find_delimiters_simd(text: &[u8]) -> Vec<DelimiterHit> {
	let quote = Simd::<u8, LANE_COUNT>::splat(b'"');
	let comma = Simd::<u8, LANE_COUNT>::splat(b',');
	let newline = Simd::<u8, LANE_COUNT>::splat(b'\n');

	let mut hits = Vec::new();
	let (prefix, middle, suffix) = text.as_simd::<LANE_COUNT>();

	for (index, &byte) in prefix.iter().enumerate() {
		if let Some(kind) = classify_byte(byte) {
			hits.push(DelimiterHit { position: index, kind });
		}
	}

	let aligned_offset = prefix.len();

	for (chunk_index, chunk) in middle.iter().enumerate() {
		let quote_mask = chunk.simd_eq(quote).to_bitmask();
		let comma_mask = chunk.simd_eq(comma).to_bitmask();
		let newline_mask = chunk.simd_eq(newline).to_bitmask();

		let mut any_mask = quote_mask | comma_mask | newline_mask;
		if any_mask == 0 {
			continue;
		}

		let base = aligned_offset + chunk_index * LANE_COUNT;

		while any_mask != 0 {
			let lane = any_mask.trailing_zeros() as usize;
			let bit = 1 << lane;

			let kind = if quote_mask & bit != 0 {
				Delimiter::Quote
			} else if comma_mask & bit != 0 {
				Delimiter::Comma
			} else {
				Delimiter::Newline
			};

			hits.push(DelimiterHit {
				position: base + lane,
				kind,
			});
			any_mask &= any_mask - 1;
		}
	}

	let suffix_offset = aligned_offset + middle.len() * LANE_COUNT;
	for (index, &byte) in suffix.iter().enumerate() {
		if let Some(kind) = classify_byte(byte) {
			hits.push(DelimiterHit {
				position: suffix_offset + index,
				kind,
			});
		}
	}

	hits
}

const fn classify_byte(byte: u8) -> Option<Delimiter> {
	match byte {
		b'"' => Some(Delimiter::Quote),
		b',' => Some(Delimiter::Comma),
		b'\n' => Some(Delimiter::Newline),
		_ => None,
	}
}

fn parse_csv(text: &[u8]) -> Vec<Vec<String>> {
	let hits = find_delimiters_simd(text);
	let mut rows = Vec::new();
	let mut fields = Vec::new();
	let mut inside_quotes = false;
	let mut field_start = 0;

	for hit in &hits {
		match (hit.kind, inside_quotes) {
			(Delimiter::Quote, _) => {
				inside_quotes = !inside_quotes;
			},
			(Delimiter::Comma, false) => {
				fields.push(extract_field(text, field_start, hit.position));
				field_start = hit.position + 1;
			},
			(Delimiter::Newline, false) => {
				fields.push(extract_field(text, field_start, hit.position));
				rows.push(std::mem::take(&mut fields));
				field_start = hit.position + 1;
			},
			// Commas and newlines inside quotes are literal content
			_ => {},
		}
	}

	// Handle final field if the file doesn't end with a newline
	if field_start < text.len() {
		fields.push(extract_field(text, field_start, text.len()));
		rows.push(fields);
	}

	rows
}

fn extract_field(text: &[u8], start: usize, end: usize) -> String {
	let mut raw = &text[start..end];

	// Strip trailing \r so CRLF line endings just work
	if raw.last() == Some(&b'\r') {
		raw = &raw[..raw.len() - 1];
	}

	// Unquoted field — take bytes as-is
	if raw.first() != Some(&b'"') {
		return String::from_utf8_lossy(raw).into_owned();
	}

	// Quoted field — strip outer quotes and unescape "" → "
	let inner = &raw[1..raw.len() - 1];
	let unescaped = String::from_utf8_lossy(inner).replace("\"\"", "\"");
	unescaped
}

pub fn foo() {
	let text = "name,age,city\n\"Alice\",30,\"New York\"\nBob,25,London\n";

	let rows = parse_csv(text.as_bytes());
	for row in &rows {
		println!("{:?}", row);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_csv_test_simple() {
		let input = b"a,b,c\n1,2,3\n";
		let rows = parse_csv(input);
		assert_eq!(
			rows,
			vec![
				vec![String::from("a"), String::from("b"), String::from("c")],
				vec![String::from("1"), String::from("2"), String::from("3")],
			]
		);
	}

	#[test]
	fn parse_csv_test_quoted_fields() {
		let input = b"\"hello\",world\n";
		let rows = parse_csv(input);
		assert_eq!(rows, vec![vec![String::from("hello"), String::from("world")],]);
	}

	#[test]
	fn parse_csv_test_comma_inside_quotes() {
		let input = b"\"one,two\",three\n";
		let rows = parse_csv(input);
		assert_eq!(rows, vec![vec![String::from("one,two"), String::from("three")],]);
	}

	#[test]
	fn parse_csv_test_newline_inside_quotes() {
		let input = b"\"line1\nline2\",other\n";
		let rows = parse_csv(input);
		assert_eq!(rows, vec![vec![String::from("line1\nline2"), String::from("other")],]);
	}

	#[test]
	fn parse_csv_test_escaped_quotes() {
		let input = b"\"say \"\"hello\"\"\",normal\n";
		let rows = parse_csv(input);
		assert_eq!(rows, vec![vec![String::from("say \"hello\""), String::from("normal")],]);
	}

	#[test]
	fn parse_csv_test_crlf() {
		let input = b"a,b\r\nc,d\r\n";
		let rows = parse_csv(input);
		assert_eq!(
			rows,
			vec![
				vec![String::from("a"), String::from("b")],
				vec![String::from("c"), String::from("d")],
			]
		);
	}

	#[test]
	fn parse_csv_test_no_trailing_newline() {
		let input = b"a,b,c";
		let rows = parse_csv(input);
		assert_eq!(rows, vec![vec![String::from("a"), String::from("b"), String::from("c")],]);
	}

	#[test]
	fn parse_csv_test_empty_fields() {
		let input = b",,,\n";
		let rows = parse_csv(input);
		assert_eq!(
			rows,
			vec![vec![
				String::from(""),
				String::from(""),
				String::from(""),
				String::from("")
			],]
		);
	}
}
