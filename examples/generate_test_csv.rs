use std::{
	fs::File,
	io::{self, BufWriter, Write},
	path::PathBuf,
};

const TARGET_BYTES: u64 = 5 * 1024 * 1024 * 1024;
// Large sequential writes benefit from a big userspace buffer
const WRITE_BUFFER_BYTES: usize = 1024 * 1024;
// Must exceed the sliding window's 4096-byte read limit so the parser
// is forced to stitch a single cell across multiple reads
const OVERSIZED_CELL_BYTES: usize = 6000;

fn main() -> io::Result<()> {
	let output_path = PathBuf::from("test.csv");
	let file = File::create(&output_path)?;
	let mut writer = BufWriter::with_capacity(WRITE_BUFFER_BYTES, file);

	let oversized_cell = "x".repeat(OVERSIZED_CELL_BYTES);
	let oversized_row = format!("big,before,{oversized_cell},after\n");

	// Every row targets at least one edge case from the parser tests.
	let fixed_rows: &[&str] = &[
		"id,name,description,notes\n",
		"1,plain,simple row,ok\n",
		"2,\"quoted\",\"also quoted\",\"end\"\n",
		// escaped quotes inside a quoted cell
		"3,\"say \"\"hi\"\"\",normal,\"goodbye \"\"friend\"\"\"\n",
		// newline embedded inside a quoted cell
		"4,\"line one\nline two\",col3,col4\n",
		// comma inside a quoted cell
		"5,\"contains, comma\",\"and, another\",tail\n",
		// umlauts (2-byte UTF-8)
		"6,Müller,Größe,Ärger\n",
		// emojis (4-byte UTF-8)
		"7,🎉,🚀,🌍\n",
		// CRLF line ending mixed in
		"8,crlf,ending,test\r\n",
		// all fields empty
		"9,,,\n",
		// some fields empty
		"10,a,,c\n",
		// trailing comma produces an empty final cell
		"11,a,b,\n",
		// whitespace preserved verbatim (leading tab, trailing spaces)
		"12,\ttabbed,normal,trailing  \n",
		// empty quoted fields, distinct from plain empty
		"13,\"\",normal,\"\"\n",
	];

	let mut bytes_written: u64 = 0;
	let mut next_progress_report: u64 = 512 * 1024 * 1024;

	while bytes_written < TARGET_BYTES {
		for row in fixed_rows {
			writer.write_all(row.as_bytes())?;
			bytes_written += row.len() as u64;
		}
		writer.write_all(oversized_row.as_bytes())?;
		bytes_written += oversized_row.len() as u64;

		if bytes_written >= next_progress_report {
			println!("written {} MiB", bytes_written / (1024 * 1024));
			next_progress_report += 512 * 1024 * 1024;
		}
	}

	writer.flush()?;
	println!("done: {bytes_written} bytes at {}", output_path.display());
	Ok(())
}
