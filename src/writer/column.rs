//! Writer for columnar CSV


use arrow::array::{Array, ArrayRef, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, FieldRef};
use bytes::{Buf,BufMut, Bytes, BytesMut};

use crate::error::Result;

pub struct ColumnWriter {
    buffer: BytesMut,
    first_batch_written: bool,
}

impl ColumnWriter {
    pub fn new(field: &FieldRef) -> Self {
        let mut buffer = BytesMut::new();
        buffer.put(field.name().as_bytes());
        buffer.put_u8(':' as u8);
        
        Self {
            first_batch_written: false,
            buffer,
        }
    }

    pub async fn append(&mut self, array: &ArrayRef) -> Result<()> {
        let cast_arr = cast(array, &DataType::Utf8)?;
        let str_arr = cast_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("can downcast to known type StringArray");

        if self.first_batch_written {
            // append an extra comma to separate batches
            self.buffer.put_u8(',' as u8);
        } else {
            self.first_batch_written = true;
        }

        for i in 0..array.len() - 1 {
            self.buffer.put(str_arr.value(i).as_bytes());
            self.buffer.put_u8(',' as u8);
        }
        self.buffer.put(str_arr.value(array.len() - 1).as_bytes());

        Ok(())
    }

    pub async fn flush(&mut self) -> Result<Bytes> {
        Ok(self.buffer.copy_to_bytes(self.buffer.len()))
    }
}