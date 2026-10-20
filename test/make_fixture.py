"""Write a small Iceberg table with PyIceberg, and PyIceberg's own read of it.

Generated rather than checked in. Iceberg metadata records absolute paths — in
`location`, in `metadata-log` and inside the Avro manifests — so a checked-in
fixture only resolves on the machine that made it.

It is also written by PyIceberg on purpose, so the scan is tested against
a table produced by the reference implementation. A fixture we wrote and then
read back would prove our writer and our reader agree and nothing more.

Next to the table it writes `oracle.parquet`: what PyIceberg returns for a
full scan, deletes applied. The SQL tests run every query twice — once over
the table through the Mojo scan, once over this file through DataFusion's own
Parquet reader — and compare. Same engine, same SQL, two scans: a difference
can only come from the scan.

Deterministic: same rows, same order, every run.

    python test/make_fixture.py test/fixtures    # prints the table directory
"""

import os
import shutil
import sys
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog

warehouse = os.path.abspath(sys.argv[1])
shutil.rmtree(warehouse, ignore_errors=True)
# Before the catalog: SqlCatalog opens its sqlite file inside the warehouse,
# and sqlite will not create the directory for it.
os.makedirs(warehouse, exist_ok=True)

catalog = SqlCatalog(
    "mlake",
    **{
        "uri": f"sqlite:///{warehouse}/catalog.db",
        "warehouse": f"file://{warehouse}",
    },
)

# `amount` carries nulls on purpose. A dropped or misaligned validity bitmap
# produces plausible numbers rather than an error, so it is the column worth
# asserting on.
schema = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("region", pa.string(), nullable=False),
        pa.field("amount", pa.float64(), nullable=True),
        pa.field("ok", pa.bool_(), nullable=False),
        pa.field("ts", pa.timestamp("us"), nullable=True),
    ]
)
rows = pa.table(
    {
        "id": [1, 2, 3, 4, 5, 6, 7],
        "region": ["eu", "us", "eu", "us", "apac", "eu", "apac"],
        "amount": [1.5, None, 3.5, 4.5, 5.5, 6.5, None],
        "ok": [True, False, True, False, True, True, False],
        "ts": [
            datetime(2023, 11, 14),
            datetime(2023, 11, 15),
            datetime(2023, 11, 16, 12, 0),
            datetime(2023, 11, 17),
            datetime(2023, 12, 1),
            datetime(2024, 1, 1),
            datetime(2023, 11, 14),
        ],
    },
    schema=schema,
)

catalog.create_namespace_if_not_exists("db")
# Uncompressed Parquet. PyIceberg defaults to zstd, and every codec in
# parquet.mojo is a dlopened shim living in its own tin's environment —
# dragging four of them in would test the codecs, which these tests are not for.
table = catalog.create_table(
    "db.taxi",
    schema=schema,
    properties={"write.parquet.compression-codec": "uncompressed"},
)

# Two appends, so the scan plans more than one data file.
table.append(rows.slice(0, 4))
table.append(rows.slice(4, 3))

# A third file with many small row groups, registered with add_files. PyIceberg
# ignores row-group-size properties on its own writes, and without several row
# groups inside one file there is nothing for split planning to divide — the
# ticket count would equal the file count and a broken splitter would pass
# unnoticed. add_files records split_offsets from the footer, which is what
# lets the planner divide the file without opening it.
wide = pa.table(
    {
        "id": list(range(100, 700)),
        "region": ["split"] * 600,
        "amount": [1.0] * 600,
        "ok": [True] * 600,
        "ts": [datetime(2024, 2, 1)] * 600,
    },
    schema=schema,
)
external = os.path.join(warehouse, "external.parquet")
pq.write_table(wide, external, row_group_size=25, compression="none")
table.add_files([external])

# A row-level delete, so the scan has to honour one: `id = 3` is in the first
# data file and must not come back.
table.delete("id = 3")
table = catalog.load_table("db.taxi")
pq.write_table(table.scan().to_arrow(), os.path.join(warehouse, "oracle.parquet"))

# The table directory, not the metadata file: the caller wants somewhere to
# point a scan at, and deriving it here keeps PyIceberg's layout from being
# duplicated — and drifting — in a shell script.
print(os.path.dirname(os.path.dirname(table.metadata_location.removeprefix("file://"))))
