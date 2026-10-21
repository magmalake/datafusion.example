"""Diff the two scans' answers, then tabulate their times.

Exits non-zero if any answer disagrees: a time is only worth reading for a
scan that returned the right rows. Counts and zones must match exactly; sums
within 1e-9 relative, because the two scans hand DataFusion the same doubles
in a different order and floating-point addition is not associative.

    python3 bench/compare.py bench/results.jsonl
"""

import json
import statistics
import sys


def cells_agree(a: str, b: str) -> bool:
    if a == b:
        return True
    try:
        x, y = float(a), float(b)
    except ValueError:
        return False
    return abs(x - y) <= 1e-9 * max(abs(x), abs(y))


def main(path: str) -> int:
    runs = {}
    for line in open(path):
        r = json.loads(line)
        runs[(r["threads"], r["query"], r["scan"])] = r

    bad = 0
    print(f"{'threads':>7}  {'query':<16} {'iceberg ms':>11} {'mlake ms':>9} {'mlake/iceberg':>14}")
    for threads, query in sorted({(t, q) for t, q, _ in runs}):
        ice, ml = runs.get((threads, query, "iceberg")), runs.get((threads, query, "mlake"))
        if not ice or not ml:
            print(f"{threads:>7}  {query:<16} missing a scan")
            bad += 1
            continue
        a, b = ice["answer"], ml["answer"]
        same = len(a) == len(b) and all(
            len(ra) == len(rb) and all(cells_agree(x, y) for x, y in zip(ra, rb))
            for ra, rb in zip(a, b)
        )
        if not same:
            bad += 1
            print(f"{threads:>7}  {query:<16} ANSWERS DIFFER\n    iceberg {a}\n    mlake   {b}")
            continue
        # The median of the runs after the first: the first pays for opening
        # the table and for whatever the page cache did not have.
        ti = statistics.median(ice["runs_ms"][1:] or ice["runs_ms"])
        tm = statistics.median(ml["runs_ms"][1:] or ml["runs_ms"])
        print(f"{threads:>7}  {query:<16} {ti:>11.1f} {tm:>9.1f} {tm / ti:>13.2f}x")
    if bad:
        print(f"\n{bad} disagreement(s): times above are not comparable", file=sys.stderr)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
