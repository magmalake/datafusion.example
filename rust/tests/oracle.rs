//! Every query twice: over the Iceberg table through the Mojo scan, and over
//! `oracle.parquet` — PyIceberg's own read of the same table — through
//! DataFusion's Parquet reader. Same engine, same SQL, two scans, so a
//! difference can only come from the scan.
//!
//! Needs the fixture: `pixi run test` generates it and sets `MLAKE_FIXTURE`
//! to the table directory. The oracle file sits two levels above it.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use mlake_datafusion::MlakeTable;
use mlake_datafusion::bridge::{self, Bridge};

fn fixture() -> PathBuf {
    let dir = std::env::var("MLAKE_FIXTURE")
        .expect("MLAKE_FIXTURE is not set; run the tests with `pixi run test`");
    PathBuf::from(dir)
}

/// The table as `taxi` (split finely, so one file is several partitions) and
/// the oracle as `oracle`.
async fn context() -> SessionContext {
    let dir = fixture();
    let oracle = dir.join("../../oracle.parquet");
    let bridge = Bridge::load(&bridge::default_path()).unwrap();
    let table = MlakeTable::open(bridge, dir.to_str().unwrap())
        .unwrap()
        .with_split_size(4096);
    let ctx = SessionContext::new();
    ctx.register_table("taxi", Arc::new(table)).unwrap();
    ctx.register_parquet("oracle", path(&oracle), ParquetReadOptions::default())
        .await
        .unwrap();
    ctx
}

fn path(p: &Path) -> &str {
    p.to_str().unwrap()
}

async fn rows(ctx: &SessionContext, sql: &str) -> String {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    pretty_format_batches(&batches).unwrap().to_string()
}

/// Run `sql` (written against `taxi`) on both tables; the results must match.
///
/// Compared as printed tables, so name expression columns with an alias: an
/// unaliased `sum(amount)` is headed with the table it came from.
async fn same(ctx: &SessionContext, sql: &str) {
    let got = rows(ctx, sql).await;
    let want = rows(ctx, &sql.replace("taxi", "oracle")).await;
    assert_eq!(got, want, "\nquery: {sql}");
}

#[tokio::test]
async fn whole_table() {
    let ctx = context().await;
    same(&ctx, "SELECT * FROM taxi ORDER BY id").await;
    same(&ctx, "SELECT count(*) FROM taxi").await;
    // The deleted row stays deleted.
    assert!(
        !rows(&ctx, "SELECT id FROM taxi WHERE id = 3")
            .await
            .contains("| 3")
    );
}

#[tokio::test]
async fn projections_in_any_order() {
    let ctx = context().await;
    same(&ctx, "SELECT amount, id FROM taxi ORDER BY id").await;
    same(&ctx, "SELECT ts, region FROM taxi ORDER BY ts, region").await;
}

#[tokio::test]
async fn aggregates() {
    let ctx = context().await;
    same(
        &ctx,
        "SELECT region, count(*) n, count(amount) amounts, sum(amount) total, min(ts) first, \
         max(ts) last FROM taxi GROUP BY region ORDER BY region",
    )
    .await;
}

#[tokio::test]
async fn pushed_down_filters() {
    let ctx = context().await;
    for w in [
        "id >= 650",
        "id < 5",
        "650 <= id",
        "amount IS NULL",
        "amount IS NOT NULL AND amount > 4",
        "region IN ('eu', 'apac') AND ok",
        "region NOT IN ('split')",
        "NOT (region = 'split') OR id = 650",
        "ts >= TIMESTAMP '2024-01-01 00:00:00'",
        "region = 'nowhere'",
    ] {
        same(&ctx, &format!("SELECT * FROM taxi WHERE {w} ORDER BY id")).await;
    }
}

#[tokio::test]
async fn filters_it_cannot_push_down_are_still_applied() {
    let ctx = context().await;
    same(
        &ctx,
        "SELECT id FROM taxi WHERE region LIKE 'e%' ORDER BY id",
    )
    .await;
    same(
        &ctx,
        "SELECT id FROM taxi WHERE id >= 600 AND region LIKE 's%' ORDER BY id",
    )
    .await;
    same(
        &ctx,
        "SELECT id FROM taxi WHERE id % 97 = 0 OR region = 'us' ORDER BY id",
    )
    .await;
}

#[tokio::test]
async fn filters_prune_tasks() {
    let ctx = context().await;
    let tasks = |plan: String| -> usize {
        let at = plan
            .find("MlakeExec: tasks=")
            .expect("an MlakeExec in the plan");
        let rest = &plan[at + "MlakeExec: tasks=".len()..];
        rest[..rest.find(|c: char| !c.is_ascii_digit()).unwrap()]
            .parse()
            .unwrap()
    };
    let all = tasks(rows(&ctx, "EXPLAIN SELECT id FROM taxi").await);
    let few = tasks(rows(&ctx, "EXPLAIN SELECT id FROM taxi WHERE id < 5").await);
    assert!(
        all > 1,
        "split planning should divide the fixture, got {all} tasks"
    );
    assert!(few < all, "id < 5 should prune tasks: {few} of {all}");
}
