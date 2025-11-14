//! Writer for columnar CSV

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use object_store::{ObjectStore, PutPayload};
use object_store::path::Path;

use crate::error::Result;
use crate::writer::column::ColumnWriter;

mod column;

pub struct WriterOptions {
    pub path: Path,
    pub object_store: Arc<dyn ObjectStore>,
    pub schema: SchemaRef,
}

pub struct Writer {
    path: Path, 
    object_store: Arc<dyn ObjectStore>,
    column_writers: Vec<ColumnWriter>,
}

impl Writer {
    pub fn new_with_options(options: &WriterOptions) -> Self {
        let column_writers = options.schema.fields()
            .iter()
            .map(|field| ColumnWriter::new(field))
            .collect();

        Self {
            column_writers,
            path: options.path.clone(),
            object_store: options.object_store.clone(),
        }
    }

    pub async fn append(&mut self, record_batch: &RecordBatch) -> Result<()> {
        let columns = record_batch.columns();
        for (array, col_writer) in columns.iter().zip(&mut self.column_writers) {
            col_writer.append(array).await?;
        }

        Ok(())
    }

    pub async fn finish(mut self) -> Result<()> {
        let mut multipart_write = self.object_store.put_multipart(&self.path).await?;

        for col_writer in &mut self.column_writers {
            let bytes = col_writer.flush().await?;
            
            // TODO check the result or log it or something?
            // TODO highly inefficient way to stick the newline in
            multipart_write.put_part(PutPayload::from_bytes(bytes)).await?;
            multipart_write.put_part(PutPayload::from_bytes(Bytes::from_static(b"\n"))).await?;
        }

        multipart_write.complete().await?;

        Ok(())
    }
}