//! Benchmark for ManifestReader performance
//!
//! Run with: cargo bench

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use iceberg_rust_native::manifest::{ContentFile, DataFile, ManifestEntry, Status};
use iceberg_rust_native::types::{FileFormat, PartitionData, PartitionValue};
use rustc_hash::FxHashMap;

/// Generate a test DataFile with realistic data
fn generate_data_file(index: usize) -> DataFile {
    let mut file = DataFile::new(
        format!("s3://bucket/data/part-{:05}.parquet", index),
        FileFormat::Parquet,
        0,
        PartitionData::new(vec![
            PartitionValue::String(format!("2024-01-{:02}", (index % 31) + 1)),
            PartitionValue::Int((index % 24) as i32),
        ]),
        10000 + (index * 100) as i64,
        1024 * 1024 * (10 + index % 90) as i64,
    );

    // Add column statistics
    let data = file.data_mut();
    data.column_sizes = Some(FxHashMap::from_iter([
        (1, 100000),
        (2, 200000),
        (3, 150000),
    ]));
    data.value_counts = Some(FxHashMap::from_iter([
        (1, 10000),
        (2, 10000),
        (3, 10000),
    ]));
    data.null_value_counts = Some(FxHashMap::from_iter([
        (1, 0),
        (2, 100),
        (3, 50),
    ]));
    data.lower_bounds = Some(FxHashMap::from_iter([
        (1, vec![0, 0, 0, 0]),
        (2, b"aaaa".to_vec()),
    ]));
    data.upper_bounds = Some(FxHashMap::from_iter([
        (1, vec![255, 255, 255, 255]),
        (2, b"zzzz".to_vec()),
    ]));

    file
}

/// Generate test ManifestEntry
fn generate_manifest_entry(index: usize) -> ManifestEntry<DataFile> {
    ManifestEntry::new(
        Status::Added,
        Some(1000 + index as i64),
        Some(index as i64),
        Some(index as i64),
        generate_data_file(index),
    )
}

/// Benchmark creating data files
fn bench_data_file_creation(c: &mut Criterion) {
    c.bench_function("create_data_file", |b| {
        b.iter(|| {
            let file = generate_data_file(black_box(42));
            black_box(file)
        })
    });
}

/// Benchmark creating manifest entries
fn bench_manifest_entry_creation(c: &mut Criterion) {
    c.bench_function("create_manifest_entry", |b| {
        b.iter(|| {
            let entry = generate_manifest_entry(black_box(42));
            black_box(entry)
        })
    });
}

/// Benchmark batch creation of entries
fn bench_batch_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_creation");
    
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let entries: Vec<_> = (0..size)
                    .map(|i| generate_manifest_entry(i))
                    .collect();
                black_box(entries)
            })
        });
    }
    
    group.finish();
}

/// Benchmark copy without stats
fn bench_copy_without_stats(c: &mut Criterion) {
    let entry = generate_manifest_entry(42);
    
    c.bench_function("copy_without_stats", |b| {
        b.iter(|| {
            let copy = entry.copy_without_stats();
            black_box(copy)
        })
    });
}

/// Benchmark filtering entries
fn bench_filter_entries(c: &mut Criterion) {
    // Create a mix of entry statuses
    let entries: Vec<_> = (0..10000)
        .map(|i| {
            let status = match i % 10 {
                0 => Status::Deleted,
                _ => Status::Added,
            };
            ManifestEntry::new(
                status,
                Some(1000 + i as i64),
                Some(i as i64),
                Some(i as i64),
                generate_data_file(i),
            )
        })
        .collect();

    c.bench_function("filter_live_entries", |b| {
        b.iter(|| {
            let live: Vec<_> = entries.iter().filter(|e| e.is_live()).collect();
            black_box(live)
        })
    });
}

/// Benchmark HashMap lookups (simulating statistics access patterns)
fn bench_stats_lookup(c: &mut Criterion) {
    let file = generate_data_file(42);
    let column_ids = vec![1, 2, 3, 4, 5];

    c.bench_function("stats_lookup", |b| {
        b.iter(|| {
            let mut total = 0i64;
            for &col_id in &column_ids {
                if let Some(sizes) = file.column_sizes() {
                    if let Some(&size) = sizes.get(&col_id) {
                        total += size;
                    }
                }
            }
            black_box(total)
        })
    });
}

criterion_group!(
    benches,
    bench_data_file_creation,
    bench_manifest_entry_creation,
    bench_batch_creation,
    bench_copy_without_stats,
    bench_filter_entries,
    bench_stats_lookup,
);

criterion_main!(benches);
