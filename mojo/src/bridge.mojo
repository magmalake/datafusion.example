"""The C ABI a DataFusion `TableProvider` calls: read an Iceberg table.

Everything here is `@export`ed and takes or returns integers, because the
other side of this boundary is Rust and the only types both languages agree on
without ceremony are addresses and lengths.

This is the Iceberg half of duckdb.extension's bridge with two additions a
query engine needs and DuckDB's table function did not ask for: a column
selection and a filter, both pushed down into the scan so that pruning happens
in the manifests and the row groups rather than after decoding.

## Why the API is stateless

The natural design would hand Rust an open scan and let it pull batches. This
Mojo cannot: `alloc` has no placement-init for a type with a destructor, and
there are no module-level globals, so there is no way to keep a `BatchReader`
alive between two calls from C.

What survives the boundary instead is a **plan** — a list of tickets, each a
string naming a snapshot, a file and a byte range — and every read is a fresh
scan of exactly one ticket. DataFusion gets one partition per ticket, and each
partition is read on its own thread.

## Strings in

Every string argument is a (pointer, length) pair of UTF-8. `columns` is a
newline-separated list of column names, empty for all of them. `filter` is the
JSON filter DSL `iceberg.expressions.parse_filter` reads, empty for none.

## Errors

A call that can fail takes `err`, the address of two words. On failure it
returns 0 and writes a message pointer and its length there; on success both
stay 0. Free the message with `mlake_free`.

## Ownership

Every address this returns is freed by a named call and by nothing else:
`mlake_plan_free` for a plan, `mlake_free` for a string, and for an
`ArrowSchema` or `ArrowArrayStream` the consumer's own `release` callback,
which is what the C Data Interface already requires.
"""

from std.memory.alloc import unsafe_alloc

from arrow_mlake.arrow import ArrayArena, ArrayData, ArrowType, AT_STRUCT
from arrow_mlake.carrow import ExportedArray, export_c
from arrow_mlake.carrow_import import release_c_array
from arrow_mlake.carrow_stream import export_stream_of
from iceberg.catalog.filesystem import find_latest_metadata
from iceberg.io import FileIO
from iceberg.metadata import TableMetadata
from iceberg.nested import empty_tree
from iceberg.read import ScanOptions
from iceberg.scan import TableScan

comptime DEFAULT_SPLIT_SIZE = 16 * 1024 * 1024
"""Target bytes per ticket when the caller does not choose.

Without splitting the unit of work is a whole data file, so one large file is
one DataFusion partition no matter how many threads are free.
"""


def _split_size(requested: Int) -> Int:
    return DEFAULT_SPLIT_SIZE if requested <= 0 else requested


# ── strings across the boundary ─────────────────────────────────────────────
def _in(ptr: Int, length: Int) -> String:
    """Copy a (pointer, length) pair from C into a Mojo string.

    A copy because the caller owns those bytes and may free them the moment
    this returns.
    """
    var bytes = List[UInt8]()
    if length <= 0:
        return String(unsafe_from_utf8=bytes)
    var p = Pointer[UInt8, MutUntrackedOrigin](unsafe_from_address=ptr)
    for i in range(length):
        bytes.append(p[unsafe_offset=i])
    return String(unsafe_from_utf8=bytes)


def _lines(text: String) -> List[String]:
    """Split on newlines, dropping empty entries."""
    var out = List[String]()
    var bytes = text.as_bytes()
    var start = 0
    for i in range(len(bytes) + 1):
        if i == len(bytes) or bytes[i] == UInt8(10):  # "\n"
            if i > start:
                out.append(String(unsafe_from_utf8=bytes[start:i]))
            start = i + 1
    return out^


def _out(s: String) -> Int:
    """Copy a Mojo string to a NUL-terminated buffer C frees with `mlake_free`.
    """
    var bytes = s.as_bytes()
    var buf = unsafe_alloc[UInt8](len(bytes) + 1)
    for i in range(len(bytes)):
        buf[unsafe_offset=i] = bytes[i]
    buf[unsafe_offset=len(bytes)] = 0
    return Int(buf)


def _fail(err: Int, message: String) -> Int:
    """Park a message on the caller's two words and return the failure value.
    """
    if err != 0:
        var slot = Pointer[Int, MutUntrackedOrigin](unsafe_from_address=err)
        slot[unsafe_offset=0] = _out(message)
        slot[unsafe_offset=1] = message.byte_length()
    return 0


def _move_into(src: Int, dst: Int, words: Int, free_shell: Bool):
    """Move a C struct into the caller's own storage.

    The caller hands us the address of an `ArrowSchema` or `ArrowArrayStream`
    it already owns — arrow-rs allocates `FFI_ArrowSchema::empty()` and
    `FFI_ArrowArrayStream::empty()` for exactly this — so returning a heap
    address would leave an allocation only this library can free.

    A stream is its own allocation and freeing its shell is right. An exported
    schema sits *inside* the block its `private_data` points at, so it is
    zeroed instead: one releasable copy, not two.
    """
    var s = Pointer[Int, MutUntrackedOrigin](unsafe_from_address=src)
    var d = Pointer[Int, MutUntrackedOrigin](unsafe_from_address=dst)
    for i in range(words):
        d[unsafe_offset=i] = s[unsafe_offset=i]
    if free_shell:
        s.unsafe_free()
    else:
        for i in range(words):
            s[unsafe_offset=i] = 0


@export("mlake_free")
def mlake_free(ptr: Int) abi("C") -> None:
    """Free one string this library returned. Not for plans or streams."""
    if ptr != 0:
        Pointer[UInt8, MutUntrackedOrigin](unsafe_from_address=ptr).unsafe_free()


# ── opening a table ─────────────────────────────────────────────────────────
def _read_text(path: String) raises -> String:
    with open(path, "r") as f:
        return f.read()


def _open(table_dir: String) raises -> TableScan:
    """A scan over whatever metadata is current in `table_dir`."""
    var io = FileIO.local()
    var meta_path = find_latest_metadata(io, table_dir)
    var metadata = TableMetadata.parse(_read_text(meta_path))
    return TableScan(metadata^, io^)


def _configure(
    var scan: TableScan, columns: String, filter: String
) raises -> TableScan:
    var cols = _lines(columns)
    var out = scan^
    if len(cols) > 0:
        out = out.select(cols^)
    if filter.byte_length() > 0:
        out = out.filter(filter)
    return out^


def _schema_into(scan: TableScan, out_ptr: Int) raises:
    """Fill the caller's `ArrowSchema` with the scan's (projected) schema.

    Built from a zero-row array of the table's type rather than from a batch,
    so a table with no rows still binds to the right columns.
    """
    var schema = scan.schema()
    var arena = ArrayArena()
    var root = empty_tree(
        arena, schema.store, schema.root, String("row"), 0, False
    )
    var exported = export_c(arena, root)
    var pair = exported.into_raw()
    release_c_array(pair[0])  # only the schema was wanted
    _move_into(pair[1], out_ptr, 9, False)  # nine words per ArrowSchema


# ── the plan ────────────────────────────────────────────────────────────────
# Layout, all 8-byte words: [n, (ptr, len) * n]. A plain array rather than a
# Mojo collection because it has to stay valid between two calls from C, and
# nothing with a destructor can.


@export("mlake_plan")
def mlake_plan(
    dir_ptr: Int,
    dir_len: Int,
    filter_ptr: Int,
    filter_len: Int,
    split_size: Int,
    err: Int,
) abi("C") -> Int:
    """Plan a scan; returns a handle whose tickets are the units of work.

    Planning reads manifests, not data files: with a filter, partitions and
    files whose column bounds cannot match are dropped here, before any
    partition is created on the Rust side.
    """
    try:
        var table_dir = _in(dir_ptr, dir_len)
        var scan = _configure(
            _open(table_dir), String(), _in(filter_ptr, filter_len)
        ).with_split_size(_split_size(split_size))
        if not scan.has_any_snapshot():
            # An empty table is not an error: zero tickets is a complete plan.
            var empty = unsafe_alloc[Int](1)
            empty[unsafe_offset=0] = 0
            return Int(empty)

        var snapshot_id = scan.snapshot().snapshot_id
        var tasks = scan.plan_files()
        var words = unsafe_alloc[Int](1 + 2 * len(tasks))
        words[unsafe_offset=0] = len(tasks)
        for i in range(len(tasks)):
            ref t = tasks[i]
            var ticket = (
                String(snapshot_id)
                + String("|")
                + String(t.start)
                + String("|")
                + String(t.length)
                + String("|")
                + t.data_file.file_path
            )
            words[unsafe_offset=1 + 2 * i] = _out(ticket)
            words[unsafe_offset=2 + 2 * i] = ticket.byte_length()
        return Int(words)
    except e:
        return _fail(err, String(e))


@export("mlake_plan_count")
def mlake_plan_count(plan: Int) abi("C") -> Int:
    if plan == 0:
        return 0
    return Pointer[Int, MutUntrackedOrigin](unsafe_from_address=plan)[]


@export("mlake_plan_ticket")
def mlake_plan_ticket(plan: Int, i: Int, len_out: Int) abi("C") -> Int:
    """The `i`th ticket. The plan owns it; do not free it separately."""
    if plan == 0:
        return 0
    var words = Pointer[Int, MutUntrackedOrigin](unsafe_from_address=plan)
    if i < 0 or i >= words[unsafe_offset=0]:
        return 0
    if len_out != 0:
        Pointer[Int, MutUntrackedOrigin](unsafe_from_address=len_out)[] = words[
            unsafe_offset=2 + 2 * i
        ]
    return words[unsafe_offset=1 + 2 * i]


@export("mlake_plan_free")
def mlake_plan_free(plan: Int) abi("C") -> None:
    if plan == 0:
        return
    var words = Pointer[Int, MutUntrackedOrigin](unsafe_from_address=plan)
    for i in range(words[unsafe_offset=0]):
        mlake_free(words[unsafe_offset=1 + 2 * i])
    words.unsafe_free()


# ── the schema ──────────────────────────────────────────────────────────────
@export("mlake_schema")
def mlake_schema(
    dir_ptr: Int, dir_len: Int, out_ptr: Int, err: Int
) abi("C") -> Int:
    """Fill the caller's `ArrowSchema` with the whole table's schema; 1 on
    success."""
    try:
        _schema_into(_open(_in(dir_ptr, dir_len)), out_ptr)
        return 1
    except e:
        return _fail(err, String(e))


# ── reading one ticket ──────────────────────────────────────────────────────
def _decode_ticket(ticket: String) raises -> Tuple[Int64, Int64, Int64, String]:
    """Split on the first three bars; the rest is the path, which may contain
    one of its own."""
    var bytes = ticket.as_bytes()
    var cuts = List[Int]()
    for i in range(len(bytes)):
        if bytes[i] == UInt8(124):  # "|"
            cuts.append(i)
            if len(cuts) == 3:
                break
    if len(cuts) < 3:
        raise Error(
            "mlake: malformed ticket, expected"
            " '<snapshot>|<start>|<length>|<path>'"
        )
    var snap = Int64(atol(String(unsafe_from_utf8=bytes[: cuts[0]])))
    var start = Int64(
        atol(String(unsafe_from_utf8=bytes[cuts[0] + 1 : cuts[1]]))
    )
    var length = Int64(
        atol(String(unsafe_from_utf8=bytes[cuts[1] + 1 : cuts[2]]))
    )
    var path = String(unsafe_from_utf8=bytes[cuts[2] + 1 :])
    return (snap, start, length, path^)


@export("mlake_read_split")
def mlake_read_split(
    dir_ptr: Int,
    dir_len: Int,
    ticket_ptr: Int,
    ticket_len: Int,
    columns_ptr: Int,
    columns_len: Int,
    filter_ptr: Int,
    filter_len: Int,
    split_size: Int,
    out_ptr: Int,
    err: Int,
) abi("C") -> Int:
    """Fill the caller's `ArrowArrayStream` with one ticket's rows; 1 on
    success.

    Planned at the ticket's own snapshot rather than at whatever is current,
    so a query that started before a commit keeps reading the table it was
    planned against. `split_size` and `filter` must be the ones the plan was
    made with: the split decides which byte range the ticket names, and the
    filter's residual is what prunes row groups inside it.
    """
    try:
        var table_dir = _in(dir_ptr, dir_len)
        var decoded = _decode_ticket(_in(ticket_ptr, ticket_len))
        var paths = List[String]()
        paths.append(decoded[3])
        var starts = List[Int64]()
        starts.append(decoded[1])

        var scan = _configure(
            _open(table_dir).use_snapshot(decoded[0]),
            _in(columns_ptr, columns_len),
            _in(filter_ptr, filter_len),
        ).with_split_size(_split_size(split_size))
        var batches = scan.to_batches_for_splits(
            paths^, starts^, ScanOptions()
        )

        var exported = List[ExportedArray]()
        for bi in range(len(batches)):
            ref b = batches[bi]
            # A record batch is a struct array with one child per column; the
            # scan hands back the columns loose, so the struct is added here.
            var row = ArrayData(ArrowType(AT_STRUCT), String("row"))
            row.nullable = False
            row.null_count = 0
            row.length = b.num_rows
            row.children = b.roots.copy()
            var root = b.arena.add(row^)
            exported.append(export_c(b.arena, root))

        if len(exported) == 0:
            # Every row of the split was deleted or filtered away. The stream
            # still has to carry the schema, so it gets one empty batch.
            var schema = scan.schema()
            var arena = ArrayArena()
            var root = empty_tree(
                arena, schema.store, schema.root, String("row"), 0, False
            )
            exported.append(export_c(arena, root))

        _move_into(export_stream_of(exported^), out_ptr, 5, True)  # five words
        return 1
    except e:
        return _fail(err, String(e))
