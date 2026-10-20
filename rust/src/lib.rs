//! A DataFusion `TableProvider` whose scan is magmalake's Mojo Iceberg reader.
//!
//! ```text
//!  DataFusion                         ArrowArrayStream            Mojo
//!  SELECT … FROM taxi ─▶ MlakeTable ─▶ plan ─▶ tickets      iceberg.mojo   planning
//!                        MlakeExec  ─▶ one partition per ticket
//!                        partition i ─▶ read_split(ticket i) ─▶ parquet.mojo decoding
//!                                                          arrow-mlake    C Data Interface
//! ```
//!
//! Planning happens in `scan`, so the number of partitions DataFusion sees
//! is the number of tickets left after the filter pruned the manifests. Each
//! partition then reads one ticket on a blocking thread, with the projection
//! and the filter passed down so that only the selected columns are decoded
//! and only row groups that can match are read.

pub mod bridge;
pub mod filter;

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatchOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::{StreamExt, TryStreamExt};

use crate::bridge::{Bridge, Ticket};

/// Bytes per ticket when not chosen: the bridge's own default.
pub const DEFAULT_SPLIT_SIZE: i64 = 0;

/// An Iceberg table on the local filesystem, read through the Mojo bridge.
#[derive(Debug)]
pub struct MlakeTable {
    bridge: Arc<Bridge>,
    table_dir: String,
    schema: SchemaRef,
    split_size: i64,
}

impl MlakeTable {
    /// Open the table at `table_dir` (the directory holding `metadata/` and
    /// `data/`). The schema is read now, once; the plan is made per query.
    pub fn open(bridge: Arc<Bridge>, table_dir: impl Into<String>) -> Result<Self> {
        let table_dir = table_dir.into();
        let schema = bridge.schema(&table_dir)?;
        Ok(Self {
            bridge,
            table_dir,
            schema,
            split_size: DEFAULT_SPLIT_SIZE,
        })
    }

    /// Divide data files into tickets of about `bytes` each, so one large
    /// file is read by several partitions.
    pub fn with_split_size(mut self, bytes: i64) -> Self {
        self.split_size = bytes;
        self
    }
}

#[async_trait]
impl TableProvider for MlakeTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Inexact even when the whole expression translates: the Mojo scan
        // prunes manifests and row groups with it, and DataFusion still
        // filters the rows. See `filter` for why that matters.
        Ok(filters
            .iter()
            .map(|f| {
                if filter::supported(f) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => Arc::clone(&self.schema),
        };
        let dsl = filter::conjunction(filters);
        let bridge = Arc::clone(&self.bridge);
        let (dir, f, split) = (self.table_dir.clone(), dsl.clone(), self.split_size);
        let tickets = tokio::task::spawn_blocking(move || bridge.plan(&dir, &f, split))
            .await
            .map_err(|e| DataFusionError::External(e.into()))??;
        Ok(Arc::new(MlakeExec::new(
            Arc::clone(&self.bridge),
            self.table_dir.clone(),
            projected,
            self.schema.field(0).name().clone(),
            dsl,
            self.split_size,
            tickets,
        )))
    }
}

/// The physical scan: one partition per ticket.
#[derive(Debug)]
pub struct MlakeExec {
    bridge: Arc<Bridge>,
    table_dir: String,
    schema: SchemaRef,
    first_column: String,
    filter: String,
    split_size: i64,
    tickets: Arc<Vec<Ticket>>,
    properties: Arc<PlanProperties>,
}

impl MlakeExec {
    fn new(
        bridge: Arc<Bridge>,
        table_dir: String,
        schema: SchemaRef,
        first_column: String,
        filter: String,
        split_size: i64,
        tickets: Vec<Ticket>,
    ) -> Self {
        // Zero tickets is still one (empty) partition: DataFusion expects at
        // least one to execute.
        let partitions = tickets.len().max(1);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            bridge,
            table_dir,
            schema,
            first_column,
            filter,
            split_size,
            tickets: Arc::new(tickets),
            properties,
        }
    }

    pub fn tickets(&self) -> usize {
        self.tickets.len()
    }
}

impl DisplayAs for MlakeExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MlakeExec: tickets={}", self.tickets.len())?;
                if !self.filter.is_empty() {
                    write!(f, ", filter={}", self.filter)?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => write!(f, "tickets={}", self.tickets.len()),
        }
    }
}

impl ExecutionPlan for MlakeExec {
    fn name(&self) -> &str {
        "MlakeExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = Arc::clone(&self.schema);
        let Some(ticket) = self.tickets.get(partition).cloned() else {
            let empty = futures::stream::empty();
            return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, empty)));
        };

        // Read only what the query uses. A query that reads no columns at
        // all — `count(*)` — still needs row counts, so it asks for one.
        let mut columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        if columns.is_empty() {
            columns.push(self.first_column.clone());
        }

        let bridge = Arc::clone(&self.bridge);
        let (dir, f, split) = (self.table_dir.clone(), self.filter.clone(), self.split_size);
        let out_schema = Arc::clone(&schema);
        let batches = futures::stream::once(async move {
            let batches = tokio::task::spawn_blocking(move || {
                bridge.read_split(&dir, &ticket, &columns, &f, split)
            })
            .await
            .map_err(|e| DataFusionError::External(e.into()))??;
            let shaped = batches
                .into_iter()
                .map(|b| conform(&b, &out_schema))
                .collect::<Result<Vec<_>>>()?;
            Ok::<_, DataFusionError>(futures::stream::iter(shaped.into_iter().map(Ok)))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            batches.boxed(),
        )))
    }
}

/// Reorder (and name-match) a batch's columns to the projected schema.
///
/// The Mojo scan returns columns in table order; DataFusion's projection may
/// ask for them in another. A batch whose schema has no fields carries only
/// its row count.
fn conform(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        let opts = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        return Ok(RecordBatch::try_new_with_options(
            Arc::clone(schema),
            vec![],
            &opts,
        )?);
    }
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let i = batch.schema().index_of(field.name())?;
        columns.push(Arc::clone(batch.column(i)));
    }
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}
