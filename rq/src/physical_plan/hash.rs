use super::{
    aggregate_expr::{Accumulator, AggregateExpr},
    expr::{Expr, PhysicalExpr},
    plan::{PhysicalPlan, Plan},
};
use crate::data_types::{column_array::ArrayRef, record_batch::RecordBatch, schema::Schema};
use anyhow::{anyhow, Error, Result};
use std::{
    any::{self, Any},
    collections::HashMap,
    fmt::Display,
};

pub(crate) struct Hash {
    input: Box<Plan>,
    schema: Schema,
    group_expr: Vec<Expr>,
    aggregate_expr: Vec<AggregateExpr>,
}

impl Hash {
    fn build_field<T: std::hash::Hash + Eq + Any + Copy>(
        &self,
        i: usize,
        group_keys: Vec<ArrayRef>,
        aggr_input_values: Vec<ArrayRef>,
    ) -> Result<ArrayRef> {
        let mut accumulator_map: HashMap<Vec<T>, Vec<Accumulator>> = HashMap::new();
        let row_key = group_keys
            .iter()
            .map(|a| {
                let value = *a
                    .get_value(i)?
                    .downcast_ref::<T>()
                    .ok_or_else(|| anyhow!("downcast value failed"))?;
                Ok::<_, Error>(value)
            })
            .collect::<Result<Vec<T>, _>>()?;
        let accumulators = accumulator_map.entry(row_key).or_insert_with(|| {
            self.aggregate_expr
                .iter()
                .map(|a| a.create_accumulator())
                .collect()
        });
        for (i, acc) in accumulators.iter_mut().enumerate() {
            let value = aggr_input_values[i].get_value(i)?;
            acc.accumulate(Some(value));
        }

        todo!()
    }
}

impl PhysicalPlan for Hash {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn execute(&self) -> anyhow::Result<Box<dyn Iterator<Item = RecordBatch> + '_>> {
        for b in self.input.execute()? {
            let group_keys: Vec<ArrayRef> = self
                .group_expr
                .iter()
                .map(|e| e.evaluate(&b))
                .collect::<Result<Vec<ArrayRef>, _>>()?;
            let aggr_input_values: Vec<ArrayRef> = self
                .aggregate_expr
                .iter()
                .map(|e| e.input_expr().evaluate(&b))
                .collect::<Result<Vec<ArrayRef>, _>>()?;
        }
        todo!()
    }

    fn children(&self) -> Vec<&Plan> {
        vec![&self.input]
    }
}

impl Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HashAggregateExec: groupExpr={}, aggrExpr={}",
            self.group_expr
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            self.aggregate_expr
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", "),
        )
    }
}
