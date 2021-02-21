// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

#[test]
fn test_indices_other() -> Result<()> {
    use super::*;
    use arrow::compute::SortOptions;
    use std::sync::Arc;

    let a = Arc::new(UInt32Array::from(vec![None, Some(1), Some(2), Some(4)]));
    let b = Arc::new(UInt32Array::from(vec![None, Some(3)]));
    let c = merge_indices(&[a], &[b], &[SortOptions::default()])?;

    // [0] false: when equal (None = None), rhs is picked
    // [1] true: None < 3
    // [2] true: 1 < 3
    // [3] true: 2 < 3
    // [4] false: 3 < 4
    // [5] true: rhs has finished => pick lhs
    assert_eq!(c, vec![false, true, true, true, false, true]);
    Ok(())
}

#[test]
fn test_indices_many() -> Result<()> {
    use super::*;
    use arrow::compute::SortOptions;
    use std::sync::Arc;

    let a1 = Arc::new(UInt32Array::from(vec![None, Some(1), Some(3)]));
    let b1 = Arc::new(UInt32Array::from(vec![None, Some(2), Some(3), Some(5)]));
    let option1 = SortOptions {
        descending: false,
        nulls_first: true,
    };

    let a2 = Arc::new(UInt32Array::from(vec![Some(2), Some(3), Some(5)]));
    let b2 = Arc::new(UInt32Array::from(vec![Some(1), Some(4), Some(6), Some(6)]));
    let option2 = SortOptions {
        descending: true,
        nulls_first: true,
    };

    let c = merge_indices(&[a1, a2], &[b1, b2], &[option1, option2])?;

    // [0] true: (N = N, 2 > 1)
    // [1] false: (1 < N, irrelevant)
    // [2] true: (1 < 2, irrelevant)
    // [3] false: (2 < 3, irrelevant)
    // [4] false: (3 = 3, 5 < 6)
    // [5] true: (3 < 5, irrelevant)
    // [6] false: lhs finished
    assert_eq!(c, vec![true, false, true, false, false, true, false]);
    Ok(())
}
