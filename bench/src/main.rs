//! The taxibench suite through DataFusion, over one of two scans.
//!
//! ```sh
//! taxibench-df --scan mlake   --threads 1 --repeat 5 <table_dir> q3_payment_sum
//! taxibench-df --scan iceberg --threads 1 --repeat 5 <table_dir> q3_payment_sum
//! ```
//!
//! taxibench.example compares implementations that each bring their own
//! aggregation, so it measures a reader *and* everything above it. This holds
//! everything above the reader fixed: the same DataFusion, the same SQL, the
//! same plan above the table scan, the same thread budget. Only the
//! `TableProvider` changes, so what is left in the difference is the scan.
//!
//! One query per process, as in taxibench: neighbouring queries warm caches
//! and allocators for each other. Prints one JSON line: every run's
//! wall-clock time, and the answer as strings for `bench/compare.py`.
//!
//! `--split-size` sets the Mojo scan's bytes per ticket (0, the default, is
//! the bridge's 16 MiB); it is ignored by the iceberg scan.

use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::TableIdent;
use iceberg::io::FileIO;
use iceberg::table::StaticTable;
use iceberg_datafusion::IcebergStaticTableProvider;
use mlake_datafusion::MlakeTable;
use mlake_datafusion::bridge::{self, Bridge};

const QUERIES: &[(&str, &str)] = &[
    (
        "q1_scan_count",
        "SELECT count(*) AS n FROM trips WHERE trip_distance > 0",
    ),
    (
        "q2_month_range",
        "SELECT count(*) AS n, sum(total_amount) AS s FROM trips \
         WHERE tpep_pickup_datetime >= TIMESTAMP '2024-03-01 00:00:00' \
           AND tpep_pickup_datetime < TIMESTAMP '2024-06-01 00:00:00'",
    ),
    (
        "q3_payment_sum",
        "SELECT count(*) AS n, sum(total_amount) AS s FROM trips WHERE payment_type = 2",
    ),
    (
        "q4_tip_ratio",
        "SELECT count(*) AS n, sum(tip_amount) AS tip, sum(fare_amount) AS fare, \
                sum(tip_amount) / sum(fare_amount) AS ratio \
         FROM trips WHERE trip_distance > 10 AND fare_amount > 0",
    ),
    (
        "q5_top_zones",
        "SELECT \"PULocationID\" AS zone, count(*) AS n FROM trips \
         WHERE trip_distance > 0 GROUP BY 1 ORDER BY n DESC, zone LIMIT 10",
    ),
    (
        "q6_zone_revenue",
        "SELECT \"PULocationID\" AS zone, sum(total_amount) AS s FROM trips \
         WHERE tpep_pickup_datetime >= TIMESTAMP '2024-01-01 00:00:00' \
           AND tpep_pickup_datetime < TIMESTAMP '2025-01-01 00:00:00' \
         GROUP BY 1 ORDER BY s DESC, zone LIMIT 10",
    ),
    // Q7 is written out per column at run time: a `count(*)` would let the
    // optimiser project every column away, and the query exists to decode
    // all nineteen.
    ("q7_wide", ""),
    (
        "q8_selective",
        "SELECT count(*) AS n, sum(total_amount) AS s FROM trips \
         WHERE \"PULocationID\" = 132 AND payment_type = 1 AND trip_distance > 20",
    ),
];

fn usage() -> ! {
    eprintln!(
        "usage: taxibench-df --scan mlake|iceberg [--threads N] [--repeat R] [--split-size BYTES] <table_dir> <query>"
    );
    std::process::exit(2);
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let (mut scan, mut threads, mut repeat, mut split) = (String::new(), 1usize, 5usize, 0i64);
    let mut positional = Vec::new();
    while let Some(a) = args.next() {
        match a.as_str() {
            "--scan" => scan = args.next().unwrap_or_else(|| usage()),
            "--threads" => {
                threads = args
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or_else(|| usage())
            }
            "--repeat" => {
                repeat = args
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or_else(|| usage())
            }
            "--split-size" => {
                split = args
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or_else(|| usage())
            }
            _ => positional.push(a),
        }
    }
    let [dir, query] = <[String; 2]>::try_from(positional).unwrap_or_else(|_| usage());
    if threads == 0 {
        threads = std::thread::available_parallelism().map_or(8, |n| n.get());
    }

    // The thread budget is the same on both sides. The runtime's workers run
    // DataFusion's operators and iceberg-datafusion's async reads; the Mojo
    // scan reads on blocking threads, so their pool gets the same cap.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .max_blocking_threads(threads)
        .enable_all()
        .build()?;
    rt.block_on(run(
        &scan,
        threads,
        repeat,
        split,
        dir.trim_end_matches('/'),
        &query,
    ))
}

async fn run(
    scan: &str,
    threads: usize,
    repeat: usize,
    split: i64,
    dir: &str,
    query: &str,
) -> Result<()> {
    let config = SessionConfig::new().with_target_partitions(threads);
    let ctx = SessionContext::new_with_config(config);
    match scan {
        "mlake" => {
            let bridge = Bridge::load(&bridge::default_path())?;
            let table = MlakeTable::open(bridge, dir)?.with_split_size(split);
            ctx.register_table("trips", Arc::new(table))?;
        }
        "iceberg" => {
            let table = StaticTable::from_metadata_file(
                &latest_metadata(dir)?,
                TableIdent::from_strs(["taxi", "trips"]).map_err(ext)?,
                FileIO::new_with_fs(),
            )
            .await
            .map_err(ext)?
            .into_table();
            let provider = IcebergStaticTableProvider::try_new_from_table(table)
                .await
                .map_err(ext)?;
            ctx.register_table("trips", Arc::new(provider))?;
        }
        _ => usage(),
    }

    let sql = match QUERIES.iter().find(|(name, _)| *name == query) {
        Some((_, "")) => wide_sql(&ctx).await?,
        Some((_, sql)) => sql.to_string(),
        // Anything else that reads as SQL is run as given: a probe, for
        // isolating one cost, not part of the suite.
        None if query
            .trim_start()
            .to_ascii_uppercase()
            .starts_with("SELECT") =>
        {
            query.to_string()
        }
        None => return Err(DataFusionError::Plan(format!("no query named {query}"))),
    };

    let mut runs_ms = Vec::with_capacity(repeat);
    let mut answer = Vec::new();
    for _ in 0..repeat.max(1) {
        let start = Instant::now();
        let batches = ctx.sql(&sql).await?.collect().await?;
        runs_ms.push(start.elapsed().as_secs_f64() * 1e3);
        answer = rows(&batches)?;
    }
    println!(
        "{{\"scan\":\"{scan}\",\"query\":\"{query}\",\"threads\":{threads},\"runs_ms\":[{}],\"answer\":{}}}",
        runs_ms
            .iter()
            .map(|m| format!("{m:.3}"))
            .collect::<Vec<_>>()
            .join(","),
        json_rows(&answer)
    );
    Ok(())
}

/// `count(c)` of every column over one month.
async fn wide_sql(ctx: &SessionContext) -> Result<String> {
    let schema = ctx.table("trips").await?.schema().clone();
    let counts: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| format!("count(\"{}\")", f.name()))
        .collect();
    Ok(format!(
        "SELECT count(*) AS n, {} FROM trips \
         WHERE tpep_pickup_datetime >= TIMESTAMP '2024-06-01 00:00:00' \
           AND tpep_pickup_datetime < TIMESTAMP '2024-07-01 00:00:00'",
        counts.join(", ")
    ))
}

/// The newest `*.metadata.json` under `<dir>/metadata`: the version numbers
/// at the front of PyIceberg's file names sort in commit order.
fn latest_metadata(dir: &str) -> Result<String> {
    let mut names: Vec<String> = std::fs::read_dir(format!("{dir}/metadata"))?
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter(|n| n.ends_with(".metadata.json"))
        .collect();
    names.sort();
    let last = names
        .pop()
        .ok_or_else(|| DataFusionError::Plan(format!("no metadata under {dir}")))?;
    Ok(format!("{dir}/metadata/{last}"))
}

fn rows(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> Result<Vec<Vec<String>>> {
    let opts = FormatOptions::default().with_null("null");
    let mut out = Vec::new();
    for b in batches {
        let formatters = b
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &opts))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for r in 0..b.num_rows() {
            out.push(formatters.iter().map(|f| f.value(r).to_string()).collect());
        }
    }
    Ok(out)
}

fn json_rows(rows: &[Vec<String>]) -> String {
    let row = |r: &Vec<String>| {
        let cells: Vec<String> = r
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\\\"")))
            .collect();
        format!("[{}]", cells.join(","))
    };
    format!("[{}]", rows.iter().map(row).collect::<Vec<_>>().join(","))
}

fn ext(e: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}
