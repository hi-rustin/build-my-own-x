use crate::{
    data_types::{
        arrow_field_array::ArrowFieldArray, column_array::ArrayRef,
        literal_value_array::LiteralValueArray, record_batch::RecordBatch,
    },
    logical_plan::expr::Operator,
};
use anyhow::Result;
use arrow::{array::Int64Array, datatypes::DataType};
use std::{any::Any, rc::Rc};

/// Physical representation of an expression.
pub(crate) trait PhysicalExpr: ToString {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef>;
}

pub(crate) enum Expr {
    Column(Column),
    Literal(ScalarValue),
    BinaryExpr(BinaryExpr),
}

impl PhysicalExpr for Expr {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef> {
        match self {
            Expr::Column(column) => column.evaluate(input),
            Expr::Literal(literal) => literal.evaluate(input),
            Expr::BinaryExpr(binary_expr) => binary_expr.evaluate(input),
        }
    }
}

impl ToString for Expr {
    fn to_string(&self) -> String {
        match self {
            Expr::Column(column) => column.to_string(),
            Expr::Literal(literal) => literal.to_string(),
            Expr::BinaryExpr(binary_expr) => binary_expr.to_string(),
        }
    }
}

pub(crate) struct Column {
    pub(crate) i: usize,
}

impl Column {
    pub(crate) fn new(i: usize) -> Self {
        Self { i }
    }
}

impl PhysicalExpr for Column {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef> {
        Ok(input.field(self.i).clone())
    }
}

impl ToString for Column {
    fn to_string(&self) -> String {
        format!("#{}", self.i)
    }
}

/// Represents a dynamically typed single value.
pub(crate) enum ScalarValue {
    String(String),
    Int64(i64),
    Float32(f32),
    Float64(f64),
}

impl PhysicalExpr for ScalarValue {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef> {
        match self {
            ScalarValue::String(s) => Ok(Rc::new(LiteralValueArray::new(
                DataType::Utf8,
                s.clone(),
                input.row_count(),
            ))),
            ScalarValue::Int64(i) => Ok(Rc::new(LiteralValueArray::new(
                DataType::Int64,
                *i,
                input.row_count(),
            ))),
            ScalarValue::Float32(f) => Ok(Rc::new(LiteralValueArray::new(
                DataType::Float32,
                *f,
                input.row_count(),
            ))),
            ScalarValue::Float64(f) => Ok(Rc::new(LiteralValueArray::new(
                DataType::Float64,
                *f,
                input.row_count(),
            ))),
        }
    }
}

impl ToString for ScalarValue {
    fn to_string(&self) -> String {
        match self {
            ScalarValue::String(s) => format!("'{}'", s),
            ScalarValue::Int64(i) => format!("{}", i),
            ScalarValue::Float32(f) => format!("{}", f),
            ScalarValue::Float64(f) => format!("{}", f),
        }
    }
}

/// For binary expressions we need to evaluate the left and right input expressions
/// and then evaluate the specific binary operator against those input values.
pub(crate) struct BinaryExpr {
    pub(crate) op: Operator,
    pub(crate) left: Box<Expr>,
    pub(crate) right: Box<Expr>,
}

impl PhysicalExpr for BinaryExpr {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef> {
        let left = self.left.evaluate(input)?;
        let right = self.right.evaluate(input)?;
        assert!(left.get_type() == right.get_type());
        let arrow_type = left.get_type();
        let mut vals = vec![];
        match self.op {
            Operator::Add => {
                for i in 0..left.size() {
                    let value = add(&left.get_value(i)?, &right.get_value(i)?, &arrow_type);
                    vals.push(value);
                }
            }
            _ => unimplemented!(),
        }

        evaluate_from_values(&vals, &arrow_type)
    }
}

impl ToString for BinaryExpr {
    fn to_string(&self) -> String {
        match self.op {
            Operator::Add => format!("{} + {}", self.left.to_string(), self.right.to_string()),
            _ => unimplemented!(),
        }
    }
}

// Build the arrow array from the values.
fn evaluate_from_values(array: &[Box<dyn Any>], data_type: &DataType) -> Result<ArrayRef> {
    match data_type {
        DataType::Int64 => {
            let arrow_array = Int64Array::from(
                array
                    .iter()
                    .map(|v| *v.downcast_ref::<i64>().unwrap())
                    .collect::<Vec<i64>>(),
            );
            Ok(Rc::new(ArrowFieldArray::new(Box::new(arrow_array))))
        }
        DataType::Float32 => {
            let arrow_array = arrow::array::Float32Array::from(
                array
                    .iter()
                    .map(|v| *v.downcast_ref::<f32>().unwrap())
                    .collect::<Vec<f32>>(),
            );
            Ok(Rc::new(ArrowFieldArray::new(Box::new(arrow_array))))
        }
        DataType::Float64 => {
            let arrow_array = arrow::array::Float64Array::from(
                array
                    .iter()
                    .map(|v| *v.downcast_ref::<f64>().unwrap())
                    .collect::<Vec<f64>>(),
            );
            Ok(Rc::new(ArrowFieldArray::new(Box::new(arrow_array))))
        }
        _ => unreachable!(),
    }
}

fn add(l: &dyn Any, r: &dyn Any, data_type: &DataType) -> Box<dyn Any> {
    match data_type {
        DataType::Int64 => {
            let l = l.downcast_ref::<i64>().unwrap();
            let r = r.downcast_ref::<i64>().unwrap();
            Box::new(*l + *r)
        }
        DataType::Float32 => {
            let l = l.downcast_ref::<f32>().unwrap();
            let r = r.downcast_ref::<f32>().unwrap();
            Box::new(*l + *r)
        }
        DataType::Float64 => {
            let l = l.downcast_ref::<f64>().unwrap();
            let r = r.downcast_ref::<f64>().unwrap();
            Box::new(*l + *r)
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::{Column, PhysicalExpr, ScalarValue};
    use crate::data_types::{
        arrow_field_array::ArrowFieldArray,
        column_array::ArrayRef,
        record_batch::RecordBatch,
        schema::{Field, Schema},
    };
    use arrow::{array::Int64Array, datatypes::DataType};
    use std::rc::Rc;

    #[test]
    fn test_column_expr_evaluate() {
        let id = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let id_arrary = vec![Rc::new(ArrowFieldArray::new(Box::new(id))) as ArrayRef];
        let schema = Schema::new(vec![Field::new("id".to_string(), DataType::Int32)]);
        let input = RecordBatch::new(schema, id_arrary);
        let expr = Column::new(0);
        assert!(expr.evaluate(&input).is_ok());
        assert!(
            expr.evaluate(&input)
                .unwrap()
                .get_value(0)
                .unwrap()
                .downcast_ref::<i64>()
                .unwrap()
                == &1
        );
    }

    #[test]
    fn test_column_expr_to_string() {
        let expr = Column::new(0);
        assert_eq!(expr.to_string(), "#0");
    }

    #[test]
    fn test_scalar_value_expr_evaluate() {
        let id = Int64Array::from(vec![1]);
        let id_arrary = vec![Rc::new(ArrowFieldArray::new(Box::new(id))) as ArrayRef];
        let schema = Schema::new(vec![Field::new("id".to_string(), DataType::Int32)]);
        let input = RecordBatch::new(schema, id_arrary);
        let expr = ScalarValue::Int64(1);
        assert!(expr.evaluate(&input).is_ok());
        assert!(
            expr.evaluate(&input)
                .unwrap()
                .get_value(0)
                .unwrap()
                .downcast_ref::<i64>()
                .unwrap()
                == &1
        );
    }

    #[test]
    fn test_scalar_value_expr_to_string() {
        let expr = ScalarValue::Int64(1);
        assert_eq!(expr.to_string(), "1");
    }
}
