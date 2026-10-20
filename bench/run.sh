#!/usr/bin/env bash
#
# The taxibench suite through DataFusion, over both scans, then a diff of the
# answers and a table of the times.
#
#   bench/run.sh                     # both legs, 5 runs per query
#   BENCH_LEG=single bench/run.sh    # one thread only
#   BENCH_REPEAT=9 bench/run.sh
#
# Needs the taxibench table (`pixi run load` in ../taxibench.example) and the
# Mojo codec shims: the table's Parquet is zstd, and zstd.mojo dlopens
# $CONDA_PREFIX/lib/libzstdmojo.so. iceberg.mojo's own default environment
# has them; point MAGMALAKE_SHIM_PREFIX elsewhere if yours do not live there.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TABLE="${BENCH_TABLE:-$ROOT/../taxibench.example/build/warehouse/warehouse/taxi/trips}"
SHIM_PREFIX="${MAGMALAKE_SHIM_PREFIX:-$ROOT/../iceberg.mojo/.pixi/envs/default}"
REPEAT="${BENCH_REPEAT:-5}"
LEGS="${BENCH_LEG:-single threaded}"
OUT="${BENCH_OUT:-$ROOT/bench/results.jsonl}"
BIN="$ROOT/bench/target/release/taxibench-df"
QUERIES=(
    q1_scan_count q2_month_range q3_payment_sum q4_tip_ratio
    q5_top_zones q6_zone_revenue q7_wide q8_selective
)

[ -d "$TABLE" ] || { echo "error: no table at $TABLE — run 'pixi run load' in taxibench.example" >&2; exit 1; }
[ -f "$SHIM_PREFIX/lib/libzstdmojo.so" ] || {
    echo "error: no libzstdmojo.so under $SHIM_PREFIX/lib — run 'pixi install' in iceberg.mojo" >&2; exit 1; }

cargo build --release -q --manifest-path bench/Cargo.toml

CORES="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 8)"
: > "$OUT"
for leg in $LEGS; do
    case "$leg" in single) T=1 ;; threaded) T="$CORES" ;; *) echo "unknown leg $leg" >&2; exit 2 ;; esac
    for q in "${QUERIES[@]}"; do
        # Alternate which scan goes first, so neither always runs on a page
        # cache the other just warmed.
        for scan in mlake iceberg; do
            CONDA_PREFIX="$SHIM_PREFIX" "$BIN" --scan "$scan" --threads "$T" --repeat "$REPEAT" "$TABLE" "$q" >> "$OUT"
        done
    done
done
python3 "$ROOT/bench/compare.py" "$OUT"
