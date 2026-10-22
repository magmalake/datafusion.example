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
type PlanFn = unsafe extern "C" fn(isize, isize, isize, isize, isize, isize, isize, isize) -> isize;
type PlanCountFn = unsafe extern "C" fn(isize) -> isize;
type PlanReadFn = unsafe extern "C" fn(isize, isize, isize, isize) -> isize;
type SchemaFn = unsafe extern "C" fn(isize, isize, isize, isize) -> isize;

/// The loaded library and the six entry points it exports.
///
/// The function pointers are copied out of the `Library` once and stay valid
/// for as long as it is loaded, which is as long as this struct lives — the
/// `_lib` field is what keeps it so.
pub struct Bridge {
    free: FreeFn,
    plan: PlanFn,
    plan_count: PlanCountFn,
    plan_read: PlanReadFn,
    plan_free: FreeFn,
    schema: SchemaFn,
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
                plan_read: sym!(b"mlake_plan_read"),
                plan_free: sym!(b"mlake_plan_free"),
                schema: sym!(b"mlake_schema"),
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

    /// Plan a scan of `columns` (all of them when empty), pruned by
    /// `filter`, the Iceberg filter DSL (empty for none).
    ///
    /// Blocking: it reads the table's metadata and manifests.
    pub fn plan(
        self: &Arc<Self>,
        table_dir: &str,
        columns: &[String],
        filter: &str,
        split_size: i64,
    ) -> Result<Plan> {
        let cols = columns.join("\n");
        let mut err = [0isize; 2];
        // SAFETY: plain integers in; a plan handle or 0 out.
        let handle = unsafe {
            (self.plan)(
                table_dir.as_ptr() as isize,
                table_dir.len() as isize,
                cols.as_ptr() as isize,
                cols.len() as isize,
                filter.as_ptr() as isize,
                filter.len() as isize,
                split_size as isize,
                err.as_mut_ptr() as isize,
            )
        };
        if handle == 0 {
            return Err(self.take_error(&err));
        }
        // SAFETY: a live handle.
        let tasks = unsafe { (self.plan_count)(handle) } as usize;
        Ok(Plan {
            bridge: Arc::clone(self),
            handle,
            tasks,
        })
    }
}

/// A planned scan, held on the Mojo side: its file scan tasks and what
/// reading them needs. Freed when dropped.
pub struct Plan {
    bridge: Arc<Bridge>,
    handle: isize,
    tasks: usize,
}

// SAFETY: the bridge only reads a plan after `mlake_plan` returns, so it may
// be read from any number of threads at once. The only write is the free in
// `drop`, which `&mut self` keeps from overlapping a read.
unsafe impl Send for Plan {}
unsafe impl Sync for Plan {}

impl std::fmt::Debug for Plan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Plan({} tasks)", self.tasks)
    }
}

impl Plan {
    /// The number of tasks: one DataFusion partition each.
    pub fn len(&self) -> usize {
        self.tasks
    }

    pub fn is_empty(&self) -> bool {
        self.tasks == 0
    }

    /// Every batch of task `index`, with the columns and the filter residual
    /// the plan was made with.
    ///
    /// Blocking: call it from a blocking thread, never from an async task.
    pub fn read(&self, index: usize) -> Result<Vec<RecordBatch>> {
        let mut stream = FFI_ArrowArrayStream::empty();
        let mut err = [0isize; 2];
        // SAFETY: the bridge moves an `ArrowArrayStream` into `stream`.
        let ok = unsafe {
            (self.bridge.plan_read)(
                self.handle,
                index as isize,
                &mut stream as *mut FFI_ArrowArrayStream as *mut c_void as isize,
                err.as_mut_ptr() as isize,
            )
        };
        if ok == 0 {
            return Err(self.bridge.take_error(&err));
        }
        let reader = ArrowArrayStreamReader::try_new(stream).map_err(|e| ffi_err("stream", e))?;
        reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| ffi_err("batch", e))
    }
}

impl Drop for Plan {
    fn drop(&mut self) {
        // SAFETY: the handle came from `mlake_plan` and is freed once.
        unsafe { (self.bridge.plan_free)(self.handle) }
    }
}
