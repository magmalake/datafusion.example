# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

This repository is an example rather than a library: nothing depends on it,
so it is not versioned.

## [Unreleased]

### Added
- **A DataFusion `TableProvider` over the Mojo Iceberg scan.** `MlakeTable`
  plans a scan into tickets through `mojo/src/bridge.mojo`, which is built as a
  shared library. `MlakeExec` reads one ticket per partition, and the batches
  cross over the Arrow C Data Interface.
- **Projection and filter pushdown.** Only the columns a query uses are
  decoded. Filters are translated into the Iceberg filter DSL and pushed down
  as `Inexact`, so they prune manifests, files and row groups while DataFusion
  still applies them.
- **`mlake-sql`**, a small CLI: `pixi run sql <table_dir> "<sql>"`, with
  `--split-size` and `--explain`.
- **Oracle tests.** Each query runs over the table through the Mojo scan and
  over PyIceberg's own read of it (`oracle.parquet`), and the two results must
  match. The fixture includes a row-level delete and a file with 24 row groups.
- **A head-to-head with iceberg-datafusion** (`bench/`, `pixi run bench`).
  It runs the taxibench suite through DataFusion 55.2 over both scans and
  checks the answers before timing. Single-threaded, the Mojo scan is
  0.78–1.62× iceberg-datafusion's time, held back by about 5 ms of
  replanning per ticket. With 10 threads it is 0.19–0.47×, because
  iceberg-datafusion's scan is a single partition.
