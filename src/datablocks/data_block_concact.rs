// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use crate::datablocks::DataBlock;
use crate::error::{FuseQueryError, FuseQueryResult};
use arrow::compute;

pub fn concat_blocks(blocks: &[DataBlock]) -> FuseQueryResult<DataBlock> {
    if blocks.is_empty() {
        return Err(FuseQueryError::Internal(
            "Can't concat empty blocks".to_string(),
        ));
    }

    let first_block = &blocks[0];
    for block in blocks.iter() {
        if block.schema().ne(first_block.schema()) {
            return Err(FuseQueryError::Internal("Schema not matched".to_string()));
        }
    }

    let mut values = Vec::with_capacity(first_block.num_columns());
    for (i, _f) in blocks[0].schema().fields().iter().enumerate() {
        let mut arr = Vec::with_capacity(blocks.len());
        for block in blocks.iter() {
            arr.push(block.column(i).as_ref());
        }
        values.push(compute::concat(&arr)?);
    }

    Ok(DataBlock::create(first_block.schema().clone(), values))
}
