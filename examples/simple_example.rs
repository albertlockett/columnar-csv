use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use columnar_csv::writer::{Writer, WriterOptions};
use object_store::local::LocalFileSystem;
use object_store::path::Path;


#[tokio::main]
async fn main() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("col_a", DataType::Utf8, false),
        Field::new("col_b", DataType::Int32, false),
    ]));

    let object_store = LocalFileSystem::new_with_prefix("/tmp").unwrap();

    let mut writer = Writer::new_with_options(&WriterOptions {
        path: Path::from("test.ccsv"),
        object_store: Arc::new(object_store),
        schema: schema.clone()
    });

    let batch1 = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(StringArray::from_iter_values(["a", "b", "c"])),
        Arc::new(Int32Array::from_iter_values([1, 2, 3]))
    ]).unwrap();

    let batch2 = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(StringArray::from_iter_values(["e", "f"])),
        Arc::new(Int32Array::from_iter_values([5, 6]))
    ]).unwrap();

    writer.append(&batch1).await.unwrap();
    writer.append(&batch2).await.unwrap();
    writer.finish().await.unwrap();
}
