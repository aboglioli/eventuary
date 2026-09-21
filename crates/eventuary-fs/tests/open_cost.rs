use std::num::NonZeroU32;
use std::time::Instant;

use eventuary_core::io::Writer;
use eventuary_core::{Event, Payload};
use eventuary_fs::writer::{FsPartitioningConfig, FsWriter, FsWriterConfig};

fn ev(key: &str) -> Event {
    Event::builder("acme", "/orders", "t", key, Payload::from_string("p"))
        .unwrap()
        .build()
        .unwrap()
}

#[tokio::test]
#[ignore = "timing report, not an assertion"]
async fn report_open_cost() {
    println!("\n  cost of FsWriter::open, by partition count\n");
    for count in [1u32, 10, 32] {
        let dir = tempfile::tempdir().unwrap();
        let config = || FsWriterConfig {
            partitioning: FsPartitioningConfig::by_event_key(NonZeroU32::new(count).unwrap()),
            ..FsWriterConfig::default()
        };

        // seed every partition so there is real state to recover
        let seed = FsWriter::open(dir.path(), config()).unwrap();
        for n in 0..500 {
            seed.write(&ev(&format!("k{n}"))).await.unwrap();
        }
        drop(seed);

        let t = Instant::now();
        for _ in 0..50 {
            let w = FsWriter::open(dir.path(), config()).unwrap();
            drop(w);
        }
        let open_only = t.elapsed().as_micros() / 50;

        let t = Instant::now();
        for n in 0..50 {
            let w = FsWriter::open(dir.path(), config()).unwrap();
            w.write(&ev(&format!("late{n}"))).await.unwrap();
            drop(w);
        }
        let open_and_write = t.elapsed().as_micros() / 50;

        println!(
            "    partitions={count:<3} open alone: {open_only:>5} µs    open + one write: {open_and_write:>5} µs"
        );
    }
    println!();
}
