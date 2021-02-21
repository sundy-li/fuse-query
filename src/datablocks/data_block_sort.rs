// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use crate::datablocks::DataBlock;
use crate::datavalues::{DataArrayRef, UInt32Array};
use crate::error::{FuseQueryError, FuseQueryResult};
use arrow::compute;
use std::sync::Arc;

pub struct SortColumnDescription {
    pub column_name: String,
    pub asc: bool,
    pub nulls_first: bool,
}

pub fn sort_block(
    block: &DataBlock,
    sort_columns_descriptions: &[SortColumnDescription],
    limit: Option<usize>,
) -> FuseQueryResult<DataBlock> {
    let order_columns = sort_columns_descriptions
        .iter()
        .map(|f| {
            Ok(compute::SortColumn {
                values: block.column_by_name(&f.column_name)?.clone(),
                options: Some(compute::SortOptions {
                    descending: !f.asc,
                    nulls_first: f.nulls_first,
                }),
            })
        })
        .collect::<FuseQueryResult<Vec<_>>>()?;

    // TODO: use pdqsort for indices sort
    let indices = compute::lexsort_to_indices(&order_columns)?;
    let mut indices_array: DataArrayRef = Arc::new(indices);
    if let Some(limit_size) = limit {
        indices_array = indices_array.clone().slice(0, limit_size);
    }
    let indices = indices_array.as_any().downcast_ref::<UInt32Array>();

    match indices {
        Some(indices) => {
            let columns = block
                .columns()
                .iter()
                .map(|c| compute::take(c.as_ref(), indices, None))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(DataBlock::create(block.schema().clone(), columns))
        }
        _ => Err(FuseQueryError::Internal(format!(
            "Cannot downcast_array from datatype:{:?} item to: UInt32Array",
            indices_array.data_type(),
        ))),
    }
}
