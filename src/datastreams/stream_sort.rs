// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::task::{Context, Poll};

use futures::stream::Stream;
use futures::StreamExt;

use crate::datablocks::sort_block;
use crate::datablocks::DataBlock;
use crate::datablocks::SortColumnDescription;
use crate::datastreams::SendableDataBlockStream;
use crate::error::FuseQueryResult;

pub struct SortStream {
    input: SendableDataBlockStream,
    sort_columns_descriptions: Vec<SortColumnDescription>,
}

impl SortStream {
    pub fn try_create(
        input: SendableDataBlockStream,
        sort_columns_descriptions: Vec<SortColumnDescription>,
    ) -> FuseQueryResult<Self> {
        Ok(SortStream {
            input,
            sort_columns_descriptions,
        })
    }
}

impl Stream for SortStream {
    type Item = FuseQueryResult<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(ctx).map(|x| match x {
            Some(Ok(v)) => Some(sort_block(&v, &self.sort_columns_descriptions, None)),
            other => other,
        })
    }
}
