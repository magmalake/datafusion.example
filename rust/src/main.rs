//! `mlake-sql`: run SQL over an Iceberg table read by the Mojo scan.
//!
//! ```sh
//! mlake-sql <table_dir> "SELECT region, count(*) FROM taxi GROUP BY region"
//! ```
//!
//! The table is registered under its directory's name (`.../db/taxi` →
//! `taxi`); `--name` overrides it, `--split-size` divides files into smaller
//! tasks, and `--explain` prints the physical plan instead of running it.

use std::sync::Arc;

use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use mlake_datafusion::MlakeTable;
use mlake_datafusion::bridge::{self, Bridge};

fn usage() -> ! {
    eprintln!("usage: mlake-sql [--name NAME] [--split-size BYTES] [--explain] <table_dir> <sql>");
    std::process::exit(2);
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let (mut name, mut split, mut explain) = (None, 0i64, false);
    let mut positional = Vec::new();
    while let Some(a) = args.next() {
        match a.as_str() {
            "--name" => name = Some(args.next().unwrap_or_else(|| usage())),
            "--split-size" => {
                split = args
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or_else(|| usage())
            }
            "--explain" => explain = true,
            "-h" | "--help" => usage(),
            _ => positional.push(a),
        }
    }
    let [dir, sql] = <[String; 2]>::try_from(positional).unwrap_or_else(|_| usage());
    let dir = dir.trim_end_matches('/').to_string();
    let name = name.unwrap_or_else(|| dir.rsplit('/').next().unwrap_or("t").to_string());

    let bridge = Bridge::load(&bridge::default_path())?;
    let table = MlakeTable::open(bridge, &dir)?.with_split_size(split);
    let ctx = SessionContext::new();
    ctx.register_table(name.as_str(), Arc::new(table))?;

    let sql = if explain {
        format!("EXPLAIN {sql}")
    } else {
        sql
    };
    ctx.sql(&sql).await?.show().await
}
