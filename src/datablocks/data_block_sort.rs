// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use crate::datablocks::DataBlock;
use crate::datavalues;
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
        indices_array = indices_array
            .clone()
            .slice(0, limit_size.min(block.num_rows()));
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

pub fn merge_sort_block(
    lhs: &DataBlock,
    rhs: &DataBlock,
    sort_columns_descriptions: &[SortColumnDescription],
    limit: Option<usize>,
) -> FuseQueryResult<DataBlock> {
    if lhs.num_rows() == 0 {
        return Ok(rhs.clone());
    }

    if rhs.num_rows() == 0 {
        return Ok(lhs.clone());
    }

    let mut sort_arrays = vec![];
    for block in [lhs, rhs].iter() {
        let columns = sort_columns_descriptions
            .iter()
            .map(|f| Ok(block.column_by_name(&f.column_name)?.clone()))
            .collect::<FuseQueryResult<Vec<_>>>()?;
        sort_arrays.push(columns);
    }

    let sort_options = sort_columns_descriptions
        .iter()
        .map(|f| {
            Ok(compute::SortOptions {
                descending: !f.asc,
                nulls_first: f.nulls_first,
            })
        })
        .collect::<FuseQueryResult<Vec<_>>>()?;

    let indices =
        datavalues::merge_indices(&sort_arrays[0], &sort_arrays[1], &sort_options, limit)?;

    let indices = match limit {
        Some(limit) => &indices[0..limit.min(indices.len())],
        _ => &indices,
    };

    let arrays = lhs
        .columns()
        .iter()
        .zip(rhs.columns().iter())
        .map(|(a, b)| datavalues::merge_array(a, b, &indices))
        .collect::<FuseQueryResult<Vec<_>>>()?;

    Ok(DataBlock::create(lhs.schema().clone(), arrays))
}

pub fn merge_sort_blocks(
    blocks: &[DataBlock],
    sort_columns_descriptions: &[SortColumnDescription],
    limit: Option<usize>,
) -> FuseQueryResult<DataBlock> {
    match blocks.len() {
        0 => Err(FuseQueryError::Internal(
            "Can't merge empty blocks".to_string(),
        )),
        1 => Ok(blocks[0].clone()),
        2 => merge_sort_block(&blocks[0], &blocks[1], sort_columns_descriptions, limit),
        _ => {
            let left = merge_sort_blocks(
                &blocks[0..blocks.len() / 2],
                sort_columns_descriptions,
                limit,
            )?;
            let right = merge_sort_blocks(
                &blocks[blocks.len() / 2..blocks.len()],
                sort_columns_descriptions,
                limit,
            )?;
            merge_sort_block(&left, &right, sort_columns_descriptions, limit)
        }
    }
}
