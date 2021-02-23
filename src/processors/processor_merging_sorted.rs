// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use async_trait::async_trait;
use futures::StreamExt;
use std::sync::Arc;

use crate::datablocks::data_block_sort::merge_sort_blocks;
use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::datavalues::DataSchemaRef;
use crate::error::FuseQueryResult;
use crate::planners::ExpressionPlan;
use crate::processors::IProcessor;
use crate::sessions::FuseQueryContextRef;
use crate::transforms::get_sort_descriptions;

pub struct MergingSortedProcessor {
    ctx: FuseQueryContextRef,
    schema: DataSchemaRef,
    exprs: Vec<ExpressionPlan>,
    limit: Option<usize>,

    list: Vec<Arc<dyn IProcessor>>,
}

impl MergingSortedProcessor {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        schema: DataSchemaRef,
        exprs: Vec<ExpressionPlan>,
        limit: Option<usize>,
    ) -> FuseQueryResult<Self> {
        Ok(MergingSortedProcessor {
            ctx,
            schema,
            exprs,
            limit,
            list: vec![],
        })
    }
}

#[async_trait]
impl IProcessor for MergingSortedProcessor {
    fn name(&self) -> &str {
        "MergingSortedProcessor"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        self.list.push(input);
        Ok(())
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        let sort_columns_descriptions =
            get_sort_descriptions(self.ctx.clone(), &self.schema, &self.exprs)?;

        let mut blocks = vec![];
        for input in self.list.iter() {
            let mut stream = input.execute().await?;
            while let Some(block) = stream.next().await {
                blocks.push(block?);
            }
        }

        let results = match blocks.len() {
            0 => vec![],
            _ => vec![merge_sort_blocks(
                &blocks,
                &sort_columns_descriptions,
                self.limit,
            )?],
        };

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            results,
        )))
    }
}
