// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::sync::Arc;

use crate::datavalues::DataSchemaRef;
use crate::planners::{ExpressionPlan, PlanNode};

#[derive(Clone)]
pub struct SortPlan {
    /// The expression to sort on
    pub order_by: Vec<ExpressionPlan>,
    /// The logical plan
    pub input: Arc<PlanNode>,
}

impl SortPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.input.schema()
    }
}
