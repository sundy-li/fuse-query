// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::cmp::Ordering;

use crate::error::FuseQueryResult;
use crate::datavalues::UInt32Array;

use arrow::array::{build_compare, make_array, ArrayRef, MutableArrayData, ArrayDataRef, DynComparator};
use arrow::compute::{SortOptions, SortColumn};
use arrow::error::ArrowError;
use arrow::error::Result;
use partial_sort::PartialSort;

/// Sort elements lexicographically from a list of `ArrayRef` into an unsigned integer
/// (`UInt32Array`) of indices.
pub fn partial_sort_to_indices(columns: &[SortColumn], limit: usize) -> Result<UInt32Array> {
    if columns.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Sort requires at least one column".to_string(),
        ));
    }

    let row_count = columns[0].values.len();
    if columns.iter().any(|item| item.values.len() != row_count) {
        return Err(ArrowError::ComputeError(
            "lexical sort columns have different row counts".to_string(),
        ));
    };

    // map to data and DynComparator
    let flat_columns = columns
        .iter()
        .map(
            |column| -> Result<(&ArrayDataRef, DynComparator, SortOptions)> {
                // flatten and convert build comparators
                // use ArrayData for is_valid checks later to avoid dynamic call
                let values = column.values.as_ref();
                let data = values.data_ref();
                Ok((
                    data,
                    build_compare(values, values)?,
                    column.options.unwrap_or_default(),
                ))
            },
        )
        .collect::<Result<Vec<(&ArrayDataRef, DynComparator, SortOptions)>>>()?;

    let lex_comparator = |a_idx: &usize, b_idx: &usize| -> Ordering {
        for (data, comparator, sort_option) in flat_columns.iter() {
            match (data.is_valid(*a_idx), data.is_valid(*b_idx)) {
                (true, true) => {
                    match (comparator)(*a_idx, *b_idx) {
                        // equal, move on to next column
                        Ordering::Equal => continue,
                        order => {
                            if sort_option.descending {
                                return order.reverse();
                            } else {
                                return order;
                            }
                        }
                    }
                }
                (false, true) => {
                    return if sort_option.nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    };
                }
                (true, false) => {
                    return if sort_option.nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    };
                }
                // equal, move on to next column
                (false, false) => continue,
            }
        }

        Ordering::Equal
    };
    let mut value_indices = (0..row_count).collect::<Vec<usize>>();
    value_indices.partial_sort(limit,lex_comparator);

    let v = value_indices
        .iter().enumerate().filter(|(i, _)| *i < limit)
        .map(|(_, v)| *v as u32).collect::<Vec<u32>>();

    Ok(UInt32Array::from(v.to_vec()))
}
