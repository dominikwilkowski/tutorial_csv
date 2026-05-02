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

		// Single loop: walk set bits in position order, classify by checking which mask owns each bit
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

fn find_delimiters_scalar(text: &[u8]) -> Vec<DelimiterHit> {
	text
		.iter()
		.enumerate()
		.filter_map(|(position, &byte)| classify_byte(byte).map(|kind| DelimiterHit { position, kind }))
		.collect()
}

pub fn foo() {
	let text = r#"name,age,city
"Alice",30,"New York"
"Bob",25,"London"
"#;

	let hits = find_delimiters_simd(text.as_bytes());
	for hit in &hits {
		println!("  {:?} at byte {}", hit.kind, hit.position);
	}

	assert_eq!(hits, find_delimiters_scalar(text.as_bytes()));
	println!("simd and scalar agree");
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn find_delimiters_simd_test_empty() {
		assert_eq!(find_delimiters_simd(b""), vec![]);
	}

	#[test]
	fn find_delimiters_simd_test_no_delimiters() {
		assert_eq!(find_delimiters_simd(b"hello world"), vec![]);
	}

	#[test]
	fn find_delimiters_simd_test_simple_csv_line() {
		let hits = find_delimiters_simd(b"a,b,c\n");
		assert_eq!(
			hits,
			vec![
				DelimiterHit {
					position: 1,
					kind: Delimiter::Comma
				},
				DelimiterHit {
					position: 3,
					kind: Delimiter::Comma
				},
				DelimiterHit {
					position: 5,
					kind: Delimiter::Newline
				},
			]
		);
	}

	#[test]
	fn find_delimiters_simd_test_quoted_field() {
		let hits = find_delimiters_simd(b"\"hello\",world\n");
		assert_eq!(
			hits,
			vec![
				DelimiterHit {
					position: 0,
					kind: Delimiter::Quote
				},
				DelimiterHit {
					position: 6,
					kind: Delimiter::Quote
				},
				DelimiterHit {
					position: 7,
					kind: Delimiter::Comma
				},
				DelimiterHit {
					position: 13,
					kind: Delimiter::Newline
				},
			]
		);
	}

	#[test]
	fn find_delimiters_simd_test_agrees_with_scalar() {
		let input = r#""name","age","city"
"Alice",30,"New York"
"Bob",25,"London"
"#
		.repeat(20);
		assert_eq!(find_delimiters_simd(input.as_bytes()), find_delimiters_scalar(input.as_bytes()),);
	}

	#[test]
	fn find_delimiters_simd_test_all_delimiters() {
		let input = "\",\n".repeat(50);
		let hits = find_delimiters_simd(input.as_bytes());
		assert_eq!(hits.len(), 150);
	}

	#[test]
	fn find_delimiters_simd_test_position_order() {
		// Verify hits come out sorted without needing a sort
		let input = r#""a","b","c"
"d","e","f"
"#
		.repeat(10);
		let hits = find_delimiters_simd(input.as_bytes());
		for window in hits.windows(2) {
			assert!(window[0].position < window[1].position);
		}
	}
}
