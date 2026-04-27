use std::{hint::black_box, path::PathBuf, time::Duration};

use criterion::{Criterion, criterion_group, criterion_main};

use csv_parse::{csv_simple, csv_sliding_window};

fn csv_parse_benchmark(criterion: &mut Criterion) {
	let path = PathBuf::from("test.csv");

	let mut group = criterion.benchmark_group("csv_parse");
	group.sample_size(20);
	group.measurement_time(Duration::from_secs(180));

	group.bench_function("simple", |bencher| {
		bencher.iter(|| {
			let csv = csv_simple::Csv::parse_file(black_box(path.clone())).expect("failed to parse test.csv");
			for row in csv {
				black_box(row);
			}
		});
	});

	group.bench_function("sliding window", |bencher| {
		bencher.iter(|| {
			let csv = csv_sliding_window::Csv::parse_file(black_box(path.clone())).expect("failed to parse test.csv");
			for row in csv {
				let row = row.expect("an error was found while parsing test.csv");
				black_box(row);
			}
		});
	});

	group.finish();
}

criterion_group!(benches, csv_parse_benchmark);
criterion_main!(benches);
