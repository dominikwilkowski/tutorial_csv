use std::{
	fs::File,
	io::Read,
	iter::{self, Peekable},
	mem::take,
	path::PathBuf,
	str,
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CsvParseError {
	UnableToOpenFile,
	UnterminatedQuote,
	CantReadUtf8,
}

struct InputState<'a> {
	chunk: &'a [u8],
	row: Vec<String>,
	cell: String,
	inside_quote: bool,
}

struct OutputState {
	csv: Vec<Vec<String>>,
	row: Vec<String>,
	cell: String,
	inside_quote: bool,
}

pub fn parse_csv(csv_file: PathBuf) -> Result<Vec<Vec<String>>, CsvParseError> {
	match File::open(csv_file) {
		Ok(mut content) => {
			// TODO: carry over bug where we split sequences into two just at the boundary `\r\n` or emojis with multiple utf-8 bytes
			let mut buffer = [0_u8; 5];
			let mut csv = Vec::new();
			let mut row = Vec::new();
			let mut cell = String::new();
			let mut inside_quote = false;

			loop {
				if let Ok(bytes_read) = content.read(&mut buffer) {
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
					}) {
						Ok(result) => {
							csv.extend(result.csv);
							row = result.row;
							cell = result.cell;
							inside_quote = result.inside_quote;
						},
						Err(error) => {
							return Err(error);
						},
					}
				} else {
					break;
				}
			}

			if inside_quote {
				return Err(CsvParseError::UnterminatedQuote);
			}

			// only emit a trailing row if the file didn't end with a newline
			if !cell.is_empty() || !row.is_empty() {
				row.push(cell);
				csv.push(row);
			}

			Ok(csv)
		},
		Err(_) => Err(CsvParseError::UnableToOpenFile),
	}
}

fn normalized_chars(chunk: &[u8]) -> Result<Peekable<impl Iterator<Item = char> + '_>, str::Utf8Error> {
	let text = str::from_utf8(chunk)?;
	let mut chars = text.chars().peekable();

	Ok(
		iter::from_fn(move || match chars.next()? {
			// \r\n: consume the \n and emit it; lone \r: emit \n in its place
			'\r' => {
				if chars.peek() == Some(&'\n') {
					chars.next()
				} else {
					Some('\n')
				}
			},
			other_char => Some(other_char),
		})
		.peekable(),
	)
}

fn parse(state: InputState) -> Result<OutputState, CsvParseError> {
	let mut output = OutputState {
		csv: Vec::new(),
		row: state.row,
		cell: state.cell,
		inside_quote: state.inside_quote,
	};

	let mut iter = normalized_chars(state.chunk).map_err(|_| CsvParseError::CantReadUtf8)?;

	while let Some(character) = iter.next() {
		match character {
			'"' if output.inside_quote && iter.peek() == Some(&'"') => {
				output.cell.push('"');
				iter.next(); // we found an escaped quote "" which we have to reduce to one, that's why we consume the second quote
			},
			'"' => output.inside_quote = !output.inside_quote,
			',' if !output.inside_quote => {
				output.row.push(take(&mut output.cell));
			},
			'\n' if !output.inside_quote => {
				output.row.push(take(&mut output.cell));
				output.csv.push(take(&mut output.row));
			},
			_ => {
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
}
