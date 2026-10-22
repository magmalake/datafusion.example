"""The C ABI a DataFusion `TableProvider` calls: read an Iceberg table.

Everything here is `@export`ed and takes or returns integers, because the
other side of this boundary is Rust and the only types both languages agree on
without ceremony are addresses and lengths.

This is the Iceberg half of duckdb.extension's bridge with two additions a
query engine needs and DuckDB's table function did not ask for: a column
selection and a filter, both pushed down into the scan so that pruning happens
in the manifests and the row groups rather than after decoding.

## A plan is an object, and reads are against it

`mlake_plan` opens the table, plans it once, and returns a handle to the
result: the file scan tasks, each one a data file (or a byte range of one),
its delete files and its residual filter, together with the schema, column
ids, partition specs and name mapping that a read needs. Each task is one
DataFusion partition, and `mlake_plan_read` reads one task from the handle.

An earlier version handed out each task as a string (snapshot, offset, path)
and kept nothing between calls, so every read reopened the metadata and
replanned the manifests to find its own task again. On the 87 tasks of the
taxibench table that cost about 3 ms per read. Holding the plan removes it:
a read goes straight to its Parquet file.

The handle is only read after `mlake_plan` returns, so any number of threads
may call `mlake_plan_read` on it at once. It is freed by `mlake_plan_free`,
which must not overlap with any read.

## Strings in

Every string argument is a (pointer, length) pair of UTF-8. `columns` is a
newline-separated list of column names, empty for all of them. `filter` is the
JSON filter DSL `iceberg.expressions.parse_filter` reads, empty for none. Both
are given once, to `mlake_plan`: the column selection decides what a read
decodes, and the filter decides which tasks there are and what each task's
residual is.

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
from iceberg.read import NameMapping, ScanOptions, ScanResult, read_data_file
from iceberg.read import is_metadata_column
from iceberg.scan import FileScanTask, TableScan
from iceberg.schema import Schema
from iceberg.transforms import PartitionSpec

comptime DEFAULT_SPLIT_SIZE = 16 * 1024 * 1024
"""Target bytes per task when the caller does not choose.

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
    """Park a message on the caller's two words and return the failure value."""
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
        Pointer[UInt8, MutUntrackedOrigin](
            unsafe_from_address=ptr
        ).unsafe_free()


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
struct _Plan(Movable):
    """Everything a read needs, resolved once from the metadata.

    These are the arguments `TableScan` would otherwise work out again on
    every read: `read_data_file` takes them loose, which is what lets a task
    be read without its scan.
    """

    var io: FileIO
    var tasks: List[FileScanTask]
    var specs: List[PartitionSpec]
    """One per task: the spec that task's data file was written with."""
    var schema: Schema
    """The snapshot's whole schema; `ids` picks the columns out of it."""
    var ids: List[Int]
    var meta_columns: List[String]
    var mapping: NameMapping
    var case_sensitive: Bool

    def __init__(out self, scan: TableScan) raises:
        self.io = scan.io.copy()
        self.tasks = scan.plan_files()
        self.specs = List[PartitionSpec]()
        for k in range(len(self.tasks)):
            var id = self.tasks[k].spec_id
            if scan.metadata.has_spec(id):
                self.specs.append(scan.metadata.spec_by_id(id))
            else:
                self.specs.append(PartitionSpec.unpartitioned(id))
        self.schema = scan.current_schema()
        self.ids = List[Int]()
        self.meta_columns = List[String]()
        if len(scan.selected) == 0:
            var cols = self.schema.columns()
            for k in range(len(cols)):
                self.ids.append(cols[k].id)
        else:
            for k in range(len(scan.selected)):
                ref name = scan.selected[k]
                if is_metadata_column(name):
                    self.meta_columns.append(name)
                else:
                    self.ids.append(self.schema.find_by_name(name).id)
        self.mapping = scan.name_mapping()
        self.case_sensitive = scan.case_sensitive


def _plan_at(plan: Int) raises -> Pointer[_Plan, MutUntrackedOrigin]:
    if plan == 0:
        raise Error("mlake: no plan")
    return Pointer[_Plan, MutUntrackedOrigin](unsafe_from_address=plan)


@export("mlake_plan")
def mlake_plan(
    dir_ptr: Int,
    dir_len: Int,
    columns_ptr: Int,
    columns_len: Int,
    filter_ptr: Int,
    filter_len: Int,
    split_size: Int,
    err: Int,
) abi("C") -> Int:
    """Plan a scan; returns a handle whose tasks are the units of work.

    Planning reads manifests, not data files: with a filter, partitions and
    files whose column bounds cannot match are dropped here, before any
    partition is created on the Rust side. An empty table is a plan with no
    tasks, not an error.
    """
    try:
        var scan = _configure(
            _open(_in(dir_ptr, dir_len)),
            _in(columns_ptr, columns_len),
            _in(filter_ptr, filter_len),
        ).with_split_size(_split_size(split_size))
        var p = unsafe_alloc[_Plan](1)
        p.unsafe_write(_Plan(scan))
        return Int(p)
    except e:
        return _fail(err, String(e))


@export("mlake_plan_count")
def mlake_plan_count(plan: Int) abi("C") -> Int:
    """How many tasks the plan has: one DataFusion partition each."""
    try:
        return len(_plan_at(plan)[].tasks)
    except:
        return 0


@export("mlake_plan_free")
def mlake_plan_free(plan: Int) abi("C") -> None:
    if plan == 0:
        return
    var p = Pointer[_Plan, MutUntrackedOrigin](unsafe_from_address=plan)
    p.unsafe_deinit_pointee()
    p.unsafe_free()


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


# ── reading one task ────────────────────────────────────────────────────────
@export("mlake_plan_read")
def mlake_plan_read(
    plan: Int, index: Int, out_ptr: Int, err: Int
) abi("C") -> Int:
    """Fill the caller's `ArrowArrayStream` with task `index`'s rows; 1 on
    success.

    Reads the plan and never changes it, so calls for different tasks (or the
    same one) may run on different threads at once.
    """
    try:
        ref p = _plan_at(plan)[]
        if index < 0 or index >= len(p.tasks):
            raise Error(
                "mlake: task " + String(index) + " of " + String(len(p.tasks))
            )
        ref t = p.tasks[index]
        var parts = read_data_file(
            p.io,
            t.data_file,
            t.delete_files,
            t.data_sequence_number,
            p.specs[index],
            p.schema,
            p.ids,
            p.meta_columns,
            p.mapping,
            t.residual,
            p.case_sensitive,
            ScanOptions(),
            t.start,
            t.length,
        )

        # Popped from the back, so reverse first to keep the file's order.
        var rev = List[ScanResult]()
        for _ in range(len(parts)):
            rev.append(parts.pop())
        var exported = List[ExportedArray]()
        while len(rev) > 0:
            var part = rev.pop()
            if part.num_rows() == 0:
                continue
            var b = part^.take_batch()
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
            # Every row of the task was deleted or filtered away. The stream
            # still has to carry the schema, so it gets one empty batch.
            var projected = p.schema.select(p.ids)
            var arena = ArrayArena()
            var root = empty_tree(
                arena, projected.store, projected.root, String("row"), 0, False
            )
            exported.append(export_c(arena, root))

        _move_into(export_stream_of(exported^), out_ptr, 5, True)  # five words
        return 1
    except e:
        return _fail(err, String(e))
