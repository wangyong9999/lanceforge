// Licensed under the Apache License, Version 2.0.
// Arrow IPC serialization/deserialization utilities.
// Used by both Coordinator (decode Worker responses) and Worker (encode responses).

use std::sync::Arc;

use arrow::array::RecordBatch;

/// Serialize a RecordBatch to Arrow IPC stream bytes.
pub fn record_batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>, arrow::error::ArrowError> {
    let mut buf = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buf)
}

/// Deserialize a RecordBatch from Arrow IPC stream bytes.
pub fn ipc_to_record_batch(data: &[u8]) -> Result<RecordBatch, arrow::error::ArrowError> {
    if data.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty())));
    }
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(data), None)?;
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty())));
    }
    arrow::compute::concat_batches(&batches[0].schema(), &batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_ipc_roundtrip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Float32, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Float32Array::from(vec![0.1, 0.2, 0.3])),
        ]).unwrap();

        let bytes = record_batch_to_ipc(&batch).unwrap();
        let decoded = ipc_to_record_batch(&bytes).unwrap();
        assert_eq!(decoded.num_rows(), 3);
    }

    #[test]
    fn test_ipc_empty() {
        let result = ipc_to_record_batch(&[]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().num_rows(), 0);
    }
}
