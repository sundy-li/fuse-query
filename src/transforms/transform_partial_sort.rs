// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;

use crate::datablocks::SortColumnDescription;
use crate::datastreams::{SendableDataBlockStream, SortStream};
use crate::datavalues::DataSchemaRef;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::planners::ExpressionPlan;
use crate::processors::{EmptyProcessor, IProcessor};
use crate::sessions::FuseQueryContextRef;

pub struct PartialSortTransform {
    ctx: FuseQueryContextRef,
    schema: DataSchemaRef,
    exprs: Vec<ExpressionPlan>,
    input: Arc<dyn IProcessor>,
}

impl PartialSortTransform {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        schema: DataSchemaRef,
        exprs: Vec<ExpressionPlan>,
    ) -> FuseQueryResult<Self> {
        Ok(PartialSortTransform {
            ctx,
            schema,
            exprs,
            input: Arc::new(EmptyProcessor::create()),
        })
    }
}

#[async_trait]
impl IProcessor for PartialSortTransform {
    fn name(&self) -> &str {
        "PartialSortTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        self.input = input;
        Ok(())
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        Ok(Box::pin(SortStream::try_create(
            self.input.execute().await?,
            get_sort_descriptions(self.ctx.clone(), &self.schema, &self.exprs)?,
        )?))
    }
}

pub fn get_sort_descriptions(
    ctx: FuseQueryContextRef,
    schema: &DataSchemaRef,
    exprs: &[ExpressionPlan],
) -> FuseQueryResult<Vec<SortColumnDescription>> {
    let mut sort_columns_descriptions = vec![];
    for x in exprs {
        match *x {
            ExpressionPlan::Sort {
                ref expr,
                asc,
                nulls_first,
            } => {
                let column_name = expr.to_field(ctx.clone(), schema)?.name().clone();
                sort_columns_descriptions.push(SortColumnDescription {
                    column_name,
                    asc,
                    nulls_first,
                });
            }
            _ => {
                return Err(FuseQueryError::Internal(format!(
                    "Sort expression must be ExpressionPlan::Sort, but got: {:?}",
                    x
                )));
            }
        }
    }
    Ok(sort_columns_descriptions)
}
