# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

This repository is an example rather than a library: nothing depends on it,
so it is not versioned.

## [Unreleased]

### Changed
- **A plan is now a handle held by the bridge.** `mlake_plan` takes the
  column selection with the filter, plans once, and returns a handle to the
  file scan tasks and everything a read needs. `mlake_plan_read` reads one
  task from it, and any number of threads may read one plan at once.
  `mlake_read_split`, `mlake_plan_ticket` and the string tickets are gone.
  Every read used to reopen the metadata and replan the manifests to find
  its task again, about 3 ms each. Single-threaded, the head-to-head is now
  0.63–1.34× iceberg-datafusion's time (was 0.78–1.62×), and 0.16–0.46× with
  10 threads.

### Added
- **A DataFusion `TableProvider` over the Mojo Iceberg scan.** `MlakeTable`
  plans a scan into file scan tasks through `mojo/src/bridge.mojo`, which is
  built as a shared library. `MlakeExec` reads one task per partition, and the batches
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
