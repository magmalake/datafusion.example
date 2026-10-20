//! The Mojo bridge, loaded at runtime: `mojo/src/bridge.mojo` built as a
//! shared library.
//!
//! Every function on the other side takes and returns 64-bit integers —
//! addresses, lengths, flags — so the declarations here are all `isize`, and
//! the conversions to Rust types happen in the safe wrappers below and
//! nowhere else.

use std::ffi::c_void;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use libloading::Library;

type FreeFn = unsafe extern "C" fn(isize);
type PlanFn = unsafe extern "C" fn(isize, isize, isize, isize, isize, isize) -> isize;
type PlanCountFn = unsafe extern "C" fn(isize) -> isize;
type PlanTicketFn = unsafe extern "C" fn(isize, isize, isize) -> isize;
type SchemaFn = unsafe extern "C" fn(isize, isize, isize, isize) -> isize;
type ReadSplitFn = unsafe extern "C" fn(
    isize,
    isize,
    isize,
    isize,
    isize,
    isize,
    isize,
    isize,
    isize,
    isize,
    isize,
) -> isize;

/// The loaded library and the seven entry points it exports.
///
/// The function pointers are copied out of the `Library` once and stay valid
/// for as long as it is loaded, which is as long as this struct lives — the
/// `_lib` field is what keeps it so.
pub struct Bridge {
    free: FreeFn,
    plan: PlanFn,
    plan_count: PlanCountFn,
    plan_ticket: PlanTicketFn,
    plan_free: FreeFn,
    schema: SchemaFn,
    read_split: ReadSplitFn,
    _lib: Library,
}

impl std::fmt::Debug for Bridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Bridge")
    }
}

/// The library `cargo build` expects to find: the one `pixi run lib` writes
/// into the sibling `mojo/` workspace. `MLAKE_BRIDGE` overrides it.
pub fn default_path() -> PathBuf {
    if let Some(p) = std::env::var_os("MLAKE_BRIDGE") {
        return PathBuf::from(p);
    }
    let ext = if cfg!(target_os = "macos") {
        "dylib"
    } else {
        "so"
    };
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../mojo/build")
        .join(format!("libmlake_bridge.{ext}"))
}

fn ffi_err(what: &str, e: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::External(format!("mlake bridge: {what}: {e}").into())
}

/// One planned unit of work: a snapshot, a byte range of one data file.
/// Opaque here — only the bridge decodes it.
#[derive(Debug, Clone)]
pub struct Ticket(pub String);

impl Bridge {
    pub fn load(path: &Path) -> Result<Arc<Self>> {
        // SAFETY: loading a library runs its initialisers; this one is the
        // Mojo bridge built from this repository.
        unsafe {
            let lib = Library::new(path).map_err(|e| ffi_err(&path.display().to_string(), e))?;
            macro_rules! sym {
                ($name:literal) => {
                    *lib.get($name).map_err(|e| ffi_err("symbol", e))?
                };
            }
            Ok(Arc::new(Self {
                free: sym!(b"mlake_free"),
                plan: sym!(b"mlake_plan"),
                plan_count: sym!(b"mlake_plan_count"),
                plan_ticket: sym!(b"mlake_plan_ticket"),
                plan_free: sym!(b"mlake_plan_free"),
                schema: sym!(b"mlake_schema"),
                read_split: sym!(b"mlake_read_split"),
                _lib: lib,
            }))
        }
    }

    /// Turn the two-word error slot into an error, freeing the message.
    fn take_error(&self, slot: &[isize; 2]) -> DataFusionError {
        if slot[0] == 0 {
            return DataFusionError::External("mlake bridge: call failed without a message".into());
        }
        // SAFETY: the bridge wrote a buffer of `slot[1]` bytes at `slot[0]`.
        let msg = unsafe {
            let bytes = std::slice::from_raw_parts(slot[0] as *const u8, slot[1] as usize);
            let s = String::from_utf8_lossy(bytes).into_owned();
            (self.free)(slot[0]);
            s
        };
        DataFusionError::External(msg.into())
    }

    /// The whole table's Arrow schema.
    pub fn schema(&self, table_dir: &str) -> Result<SchemaRef> {
        let mut ffi = FFI_ArrowSchema::empty();
        let mut err = [0isize; 2];
        // SAFETY: the bridge moves an `ArrowSchema` into `ffi`, which owns it
        // from then on and releases it when dropped.
        let ok = unsafe {
            (self.schema)(
                table_dir.as_ptr() as isize,
                table_dir.len() as isize,
                &mut ffi as *mut FFI_ArrowSchema as isize,
                err.as_mut_ptr() as isize,
            )
        };
        if ok == 0 {
            return Err(self.take_error(&err));
        }
        let schema = Schema::try_from(&ffi).map_err(|e| ffi_err("schema", e))?;
        Ok(Arc::new(schema))
    }

    /// Plan a scan into tickets. `filter` is the Iceberg filter DSL, empty
    /// for none.
    pub fn plan(&self, table_dir: &str, filter: &str, split_size: i64) -> Result<Vec<Ticket>> {
        let mut err = [0isize; 2];
        // SAFETY: plain integers in; a plan handle or 0 out.
        let plan = unsafe {
            (self.plan)(
                table_dir.as_ptr() as isize,
                table_dir.len() as isize,
                filter.as_ptr() as isize,
                filter.len() as isize,
                split_size as isize,
                err.as_mut_ptr() as isize,
            )
        };
        if plan == 0 {
            return Err(self.take_error(&err));
        }
        // SAFETY: tickets are owned by the plan and copied out before it is
        // freed.
        unsafe {
            let n = (self.plan_count)(plan);
            let mut out = Vec::with_capacity(n as usize);
            for i in 0..n {
                let mut len: isize = 0;
                let p = (self.plan_ticket)(plan, i, &mut len as *mut isize as isize);
                let bytes = std::slice::from_raw_parts(p as *const u8, len as usize);
                out.push(Ticket(String::from_utf8_lossy(bytes).into_owned()));
            }
            (self.plan_free)(plan);
            Ok(out)
        }
    }

    /// Every batch of one ticket, with only `columns` (all of them when
    /// empty) and with `filter` pruning row groups inside the split.
    ///
    /// Blocking: call it from a blocking thread, never from an async task.
    pub fn read_split(
        &self,
        table_dir: &str,
        ticket: &Ticket,
        columns: &[String],
        filter: &str,
        split_size: i64,
    ) -> Result<Vec<RecordBatch>> {
        let cols = columns.join("\n");
        let mut stream = FFI_ArrowArrayStream::empty();
        let mut err = [0isize; 2];
        // SAFETY: the bridge moves an `ArrowArrayStream` into `stream`.
        let ok = unsafe {
            (self.read_split)(
                table_dir.as_ptr() as isize,
                table_dir.len() as isize,
                ticket.0.as_ptr() as isize,
                ticket.0.len() as isize,
                cols.as_ptr() as isize,
                cols.len() as isize,
                filter.as_ptr() as isize,
                filter.len() as isize,
                split_size as isize,
                &mut stream as *mut FFI_ArrowArrayStream as *mut c_void as isize,
                err.as_mut_ptr() as isize,
            )
        };
        if ok == 0 {
            return Err(self.take_error(&err));
        }
        let reader = ArrowArrayStreamReader::try_new(stream).map_err(|e| ffi_err("stream", e))?;
        reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| ffi_err("batch", e))
    }
}
