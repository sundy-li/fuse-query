// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_pipeline_builder() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;

    use crate::processors::*;
    use crate::sql::*;

    let ctx = crate::sessions::FuseQueryContext::try_create()?;

    let plan = PlanParser::create(ctx.clone()).build_from_sql(
        "select sum(number+1)+2 as sumx from system.numbers_mt(80000) where (number+1)=4 limit 1",
    )?;
    let pipeline = PipelineBuilder::create(ctx.clone(), plan).build()?;
    let expect = "\
    \n  └─ LimitTransform × 1 processor\
    \n    └─ AggregateFinalTransform × 1 processor\
    \n      └─ Merge (AggregatePartialTransform × 8 processors) to (MergeProcessor × 1)\
    \n        └─ AggregatePartialTransform × 8 processors\
    \n          └─ FilterTransform × 8 processors\
    \n            └─ SourceTransform × 8 processors";
    let actual = format!("{:?}", pipeline);
    assert_eq!(expect, actual);

    let plan = PlanParser::create(ctx.clone()).build_from_sql(
        "select number from system.numbers_mt(80000) where (number+1)=4 order by number desc limit 10",
    )?;
    let pipeline = PipelineBuilder::create(ctx.clone(), plan).build()?;
    let expect = "\
    \n  └─ LimitTransform × 1 processor\
    \n    └─ MergingSortedProcessor × 1 processor\
    \n      └─ MergingSortTransform × 8 processors\
    \n        └─ PartialSortTransform × 8 processors\
    \n          └─ ProjectionTransform × 8 processors\
    \n            └─ FilterTransform × 8 processors\
    \n              └─ SourceTransform × 8 processors";
    let actual = format!("{:?}", pipeline);
    assert_eq!(expect, actual);
    Ok(())
}
