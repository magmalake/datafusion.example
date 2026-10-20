# datafusion.example

> Part of [**magmalake**](https://magmalake.org) — data lake building blocks in Mojo.

SQL over Apache Iceberg tables from Rust, with
[DataFusion](https://datafusion.apache.org) as the query engine and
magmalake's Mojo stack as the scan. DataFusion parses, plans, filters, joins
and aggregates. [iceberg.mojo](https://github.com/magmalake/iceberg.mojo)
plans the files, [parquet.mojo](https://github.com/magmalake/parquet.mojo)
decodes them, and the batches cross into Rust over the Arrow C Data Interface
without being copied.

```sh
pixi run sql test/fixtures/db/taxi \
  "SELECT region, count(*) n, sum(amount) s FROM taxi GROUP BY region ORDER BY region"
```

```
+--------+-----+-------+
| region | n   | s     |
+--------+-----+-------+
| apac   | 2   | 5.5   |
| eu     | 2   | 8.0   |
| split  | 600 | 600.0 |
| us     | 2   | 4.5   |
+--------+-----+-------+
```

In magmalake's other Rust integrations the Rust code is a library called from
Mojo: [iceberg-rs.mojo](https://github.com/magmalake/iceberg-rs.mojo) wraps
iceberg-rust, and restate.mojo wraps the Restate SDK. This example goes the
other way: the Rust program is the host, and the Mojo code is the library it
loads. It is the same shape as
[duckdb.extension](https://github.com/magmalake/duckdb.extension), with
DataFusion in place of DuckDB.

## How it fits together

```
  DataFusion (Rust)                  ArrowArrayStream               Mojo (shared library)
  ─────────────────                  ────────────────               ─────────────────────
  SELECT … WHERE id < 5
    MlakeTable::scan  ──filter──▶  mlake_plan            iceberg.mojo   manifests pruned
                      ◀─tickets─                                        into tickets
    MlakeExec
      partition 0 ──ticket, columns, filter──▶ mlake_read_split   parquet.mojo   row groups
      partition 1 ──ticket, columns, filter──▶ mlake_read_split                  pruned, decoded
      …                ◀── batches, in place ──                   arrow-mlake    C Data Interface
```

- **`mojo/src/bridge.mojo`** is the C interface, built with
  `mojo build --emit shared-lib`. It is the Iceberg half of
  duckdb.extension's bridge, plus the column selection and filter that a
  query engine pushes down. It takes and returns only integers (addresses and
  lengths), and it is stateless. A scan is planned into **tickets**, each one
  a snapshot plus a byte range of one data file, and every read is a fresh
  scan of one ticket. The module docstring explains why: this Mojo has no
  globals and cannot keep a reader alive between two calls from C.
- **`rust/src/bridge.rs`** loads the library with `libloading` and wraps each
  call. Arrow structures are allocated on the Rust side
  (`FFI_ArrowSchema::empty()`, `FFI_ArrowArrayStream::empty()`); the bridge
  moves its export into them, and they are freed by their own `release`
  callbacks.
- **`rust/src/lib.rs`** is the `TableProvider` and its `ExecutionPlan`.
  Planning happens in `scan`, so DataFusion sees one partition per ticket
  that survived the filter. Each partition reads its ticket on a blocking
  thread, asking only for the columns the query uses.
- **`rust/src/filter.rs`** translates DataFusion expressions into the Iceberg
  filter DSL the Mojo scan reads (`["and", ["<", "id", 5], ["=", "ok",
  true]]`). It covers comparisons, `IN`, `IS NULL`, `AND`/`OR`/`NOT`,
  numbers, strings, booleans, dates and timestamps.

### Filters are pushed down as hints

Every filter the provider takes is reported to DataFusion as `Inexact`. The
Mojo scan uses it to prune manifests, files and row groups, and DataFusion
still evaluates it on the rows that come back. A pruning mistake can
therefore only cost speed, never correctness, provided the translation never
drops a row the SQL would keep:

- An `AND` may leave out a part it cannot express: `id < 5 AND region LIKE
  'e%'` pushes `id < 5` and leaves the `LIKE` to DataFusion.
- An `OR` or a `NOT` is translated whole or not at all.

```sh
pixi run sql --split-size 4096 --explain test/fixtures/db/taxi "SELECT id FROM taxi WHERE id < 5"
#   MlakeExec: tickets=1, filter=["<","id",5]        (6 tickets without the filter)
```

## Running it

```sh
pixi run test     # builds the bridge, writes the fixture, runs the tests
pixi run build    # bridge + release build of the Rust crate
pixi run sql <table_dir> "<sql>"    # options: --name, --split-size, --explain
```

The table is registered under its directory's name, so `…/db/taxi` is
queried as `taxi`. `MLAKE_BRIDGE` points the Rust side at a bridge library
other than the one in `mojo/build/`.

The bridge is built from the source trees of the other magmalake repos,
checked out beside this one (`../iceberg.mojo`, `../parquet.mojo`, …), as in
duckdb.extension. CI clones them.

## How it is tested

`test/make_fixture.py` writes a small Iceberg table with PyIceberg: two
appends, a third file with 24 row groups so that split planning has something
to divide, and a row-level delete. It also writes `oracle.parquet`, which is
PyIceberg's own read of that table with the delete applied.

`rust/tests/oracle.rs` runs every query twice. The first run goes over the
table through the Mojo scan, and the second over `oracle.parquet` through
DataFusion's own Parquet reader. The engine and the SQL are the same, so any
difference can only come from the scan.

The queries cover:
- full scans, including the deleted row;
- projections in any order;
- grouped aggregates over nulls and timestamps;
- ten pushed-down filters;
- filters that cannot be pushed down;
- a check that a filter really reduces the number of tickets.

Swapping `<` for `>` in the translation makes the suite fail, which shows the
comparison is not vacuous.

## Limits

- **Local filesystem tables only.** The bridge opens tables with
  `FileIO.local()` and reads metadata with `find_latest_metadata`. The scan
  itself already reads object storage through objectstore.mojo; passing a
  catalog and credentials across the boundary is the next step.
- **Flat schemas are what the fixture exercises.** Nested columns cross the C
  Data Interface (iceberg.mojo tests them against pyarrow), but the filter
  translation only addresses top-level columns.
- **`LIMIT` is not pushed down.** Each ticket is read whole, and DataFusion
  stops pulling once it has enough rows.
- **No statistics.** DataFusion plans joins without row counts from the
  manifests, though they are there to be reported.
- **Read only.** iceberg.mojo writes tables, but `insert_into` is not wired
  up.

A natural next experiment is a head-to-head with iceberg-rust's own
DataFusion integration (`iceberg-datafusion`) over the
[taxibench](https://github.com/magmalake/taxibench.example) table. The two
would share an engine and the same queries, and differ only in the scan.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
