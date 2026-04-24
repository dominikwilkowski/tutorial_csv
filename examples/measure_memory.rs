use std::{
	alloc::{GlobalAlloc, Layout, System},
	path::PathBuf,
	sync::atomic::{AtomicUsize, Ordering},
};

use csv_parse::{csv_simple, csv_sliding_window};

struct TrackingAllocator;

static CURRENTLY_ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static PEAK_ALLOCATED: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for TrackingAllocator {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		let pointer = unsafe { System.alloc(layout) };
		if !pointer.is_null() {
			let after_alloc = CURRENTLY_ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
			// climb the peak with a CAS loop so concurrent allocs don't clobber each other's updates
			let mut observed_peak = PEAK_ALLOCATED.load(Ordering::Relaxed);
			while after_alloc > observed_peak {
				match PEAK_ALLOCATED.compare_exchange_weak(observed_peak, after_alloc, Ordering::Relaxed, Ordering::Relaxed) {
					Ok(_) => break,
					Err(actual) => observed_peak = actual,
				}
			}
		}
		pointer
	}

	unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
		unsafe { System.dealloc(pointer, layout) };
		CURRENTLY_ALLOCATED.fetch_sub(layout.size(), Ordering::Relaxed);
	}
}

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

fn measure<ParseFunction, ParseResult>(label: &str, parse: ParseFunction)
where
	ParseFunction: FnOnce() -> ParseResult,
{
	// Anchor the peak to whatever is already allocated (stdout buffers, etc.)
	// so we report parse-induced memory, not absolute process memory.
	let baseline = CURRENTLY_ALLOCATED.load(Ordering::Relaxed);
	PEAK_ALLOCATED.store(baseline, Ordering::Relaxed);

	let _parsed = parse();

	let peak = PEAK_ALLOCATED.load(Ordering::Relaxed);
	let peak_mib = peak.saturating_sub(baseline) as f64 / (1024.0 * 1024.0);
	println!("{label:20} peak above baseline = {peak_mib:.2} MiB");
	// _parsed drops here, freeing memory before the next measurement
}

fn main() {
	let path = PathBuf::from("test.csv");

	measure("simple:", || csv_simple::parse_csv(path.clone()));
	measure("sliding window:", || csv_sliding_window::parse_csv(path.clone()));
}
