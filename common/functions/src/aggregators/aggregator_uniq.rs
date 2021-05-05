// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::DataValueArithmetic;
use common_datavalues::DataValueArithmeticOperator;
use common_exception::{Result, ErrorCodes};

use crate::IFunction;
use crate::LiteralFunction;
use std::collections::HashSet;

#[derive(Clone)]
pub struct AggregatorUniqFunction {
    display_name: String,
    depth: usize,
    arg: Box<dyn IFunction>,

    state: HashSet<DataValue>
}

impl AggregatorUniqFunction {
    pub fn try_create(
        display_name: &str,
        args: &[Box<dyn IFunction>]
    ) -> Result<Box<dyn IFunction>> {

        match args.len() {
            1 => Ok(Box::new(AggregatorUniqFunction {
                display_name: display_name.to_string(),
                depth: 0,
                arg: args[0].clone(),
                state: HashSet::new(),
            })),
            _ => Result::Err(ErrorCodes::BadArguments(format!(
                "Function Error: Aggregator function {} args require single argument",
                display_name
            )))
        }
    }
}

impl IFunction for AggregatorUniqFunction {
    fn name(&self) -> &str {
        "AggregatorUniqFunction"
    }

    fn return_type(&self, _: &DataSchema) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, block: &DataBlock) -> Result<DataColumnarValue> {
        self.arg.eval(block)
    }

    fn set_depth(&mut self, depth: usize) {
        self.depth = depth;
    }

    fn accumulate(&mut self, block: &DataBlock) -> Result<()> {
        let rows = block.num_rows();
        let val = self.arg.eval(&block)?;

        match val {
            DataColumnarValue::Array(array) => {
                (0..rows).map(|index| {
                    self.state.insert(DataValue::try_from_array(&array, index).unwrap());
                })
            }
            DataColumnarValue::Scalar(v) => {
                self.state.insert(v);
            }
        }
        Ok(())
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        Ok(self.state.iter().map(|v| v.clone()).collect::<Vec<DataValue>>())
    }

    fn merge(&mut self, states: &[DataValue]) -> Result<()> {
        states.iter().map(|v| {
            self.state.insert(v.clone());
        });
        Ok(())
    }

    fn merge_result(&self) -> Result<DataValue> {
        Ok(DataValue::UInt64(Some(self.state.len() as u64)))
    }

    fn is_aggregator(&self) -> bool {
        true
    }
}

impl fmt::Display for AggregatorUniqFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}({})", self.display_name, self.arg)
    }
}
