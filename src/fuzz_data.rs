// Copyright 2022 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::marker::PhantomData;
use datafusion::arrow::array::{Array, BinaryBuilder, BooleanBuilder, Date32Builder, DecimalBuilder, PrimitiveBuilder, StringBuilder, Time32MillisecondBuilder, TimestampMillisecondBuilder};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Schema, TimeUnit};
use datafusion::arrow::datatypes::{Int8Type, Int16Type, Int32Type, Int64Type, Float32Type, Float64Type, BooleanType};
use datafusion::arrow::error::{ArrowError, Result};
use datafusion::arrow::record_batch::RecordBatch;
use rand::rngs::ThreadRng;
use rand::{Rng, RngCore};
use std::sync::Arc;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

/// Generate an Arrow RecordBatch for the provided schema and row count
pub fn generate_batch(
    rng: &mut ThreadRng,
    schema: &Schema,
    row_count: usize,
) -> Result<RecordBatch> {
    let gen: Vec<Arc<dyn ArrayGenerator>> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Boolean => Ok(Arc::new(BooleanGenerator {}) as Arc<dyn ArrayGenerator>),
            DataType::Int8 => Ok(Arc::new(PrimitiveGenerator::<Int8Type> { phantom: PhantomData }) as Arc<dyn ArrayGenerator>),
            DataType::Int16 => Ok(Arc::new(PrimitiveGenerator::<Int16Type> { phantom: PhantomData }) as Arc<dyn ArrayGenerator>),
            DataType::Int32 => Ok(Arc::new(PrimitiveGenerator::<Int32Type> { phantom: PhantomData }) as Arc<dyn ArrayGenerator>),
            DataType::Int64 => Ok(Arc::new(PrimitiveGenerator::<Int64Type> { phantom: PhantomData }) as Arc<dyn ArrayGenerator>),
            DataType::Float32 => Ok(Arc::new(PrimitiveGenerator::<Float32Type> { phantom: PhantomData }) as Arc<dyn ArrayGenerator>),
            DataType::Float64 => Ok(Arc::new(PrimitiveGenerator::<Float64Type> { phantom: PhantomData }) as Arc<dyn ArrayGenerator>),
            DataType::Utf8 => Ok(Arc::new(StringGenerator {}) as Arc<dyn ArrayGenerator>),
            DataType::Binary => Ok(Arc::new(BinaryGenerator {}) as Arc<dyn ArrayGenerator>),
            DataType::Date32 => Ok(Arc::new(Date32Generator {}) as Arc<dyn ArrayGenerator>),
            DataType::Time32(TimeUnit::Millisecond) => Ok(Arc::new(Time32MillisecondGenerator {}) as Arc<dyn ArrayGenerator>),
            DataType::Timestamp(TimeUnit::Millisecond, None) => Ok(Arc::new(TimestampMillisecondGenerator {}) as Arc<dyn ArrayGenerator>),
            DataType::Decimal(precision, scale) => Ok(Arc::new(DecimalGenerator { precision: *precision, scale: *scale }) as Arc<dyn ArrayGenerator>),
            _ => Err(ArrowError::SchemaError("Unsupported data type".to_string())),
        })
        .collect::<Result<Vec<_>>>()?;

    let arrays = gen
        .iter()
        .map(|g| g.generate(rng, row_count))
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(Arc::new(schema.clone()), arrays)
}

trait ArrayGenerator {
    fn generate(&self, rng: &mut ThreadRng, n: usize) -> Result<Arc<dyn Array>>;
}

struct StringGenerator {}

impl ArrayGenerator for StringGenerator {
    fn generate(&self, rng: &mut ThreadRng, n: usize) -> Result<Arc<dyn Array>> {
        let mut builder = StringBuilder::new(n);
        for i in 0..n {
            if i % 7 == 0 {
                builder.append_null()?;
            } else {
                let mut str = String::new();
                for _ in 0..8 {
                    let ch = rng.gen_range(32..127); // printable ASCII chars
                    str.push(char::from_u32(ch).unwrap());
                }
                builder.append_value(str)?;
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}

struct BooleanGenerator {}

impl ArrayGenerator for BooleanGenerator {
    fn generate(&self, rng: &mut ThreadRng, n: usize) -> Result<Arc<dyn Array>> {
        let mut builder = BooleanBuilder::new(n);
        for i in 0..n {
            if i % 5 == 0 {
                builder.append_null()?;
            } else {
                builder.append_value(rng.gen())?;
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}

pub trait RandomGen {
    type Value;
    fn gen_random(rng: &mut ThreadRng) -> Self::Value;
}

impl RandomGen for Int8Type {
    type Value = i8;
    fn gen_random(rng: &mut ThreadRng) -> Self::Value { rng.gen() }
}

impl RandomGen for Int16Type {
    type Value = i16;
    fn gen_random(rng: &mut ThreadRng) -> Self::Value { rng.gen() }
}

impl RandomGen for Int32Type {
    type Value = i32;
    fn gen_random(rng: &mut ThreadRng) -> Self::Value { rng.gen() }
}

impl RandomGen for Int64Type {
    type Value = i64;
    fn gen_random(rng: &mut ThreadRng) -> Self::Value { rng.gen() }
}

impl RandomGen for Float32Type {
    type Value = f32;
    fn gen_random(rng: &mut ThreadRng) -> Self::Value { rng.gen() }
}

impl RandomGen for Float64Type {
    type Value = f64;
    fn gen_random(rng: &mut ThreadRng) -> Self::Value { rng.gen() }
}

impl RandomGen for BooleanType {
    type Value = bool;
    fn gen_random(rng: &mut ThreadRng) -> Self::Value { rng.gen() }
}

struct PrimitiveGenerator<T>
    where
        T: ArrowPrimitiveType + RandomGen<Value = <T as ArrowPrimitiveType>::Native>,
{
    phantom: PhantomData<T>,
}

impl<T> ArrayGenerator for PrimitiveGenerator<T>
    where
        T: ArrowPrimitiveType + RandomGen<Value = <T as ArrowPrimitiveType>::Native>,
{
    fn generate(&self, rng: &mut ThreadRng, n: usize) -> Result<Arc<dyn Array>> {
        let mut builder = PrimitiveBuilder::<T>::new(n);
        for i in 0..n {
            if i % 5 == 0 {
                builder.append_null()?;
            } else {
                let value = T::gen_random(rng);
                builder.append_value(value)?;
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}

struct BinaryGenerator {}

impl ArrayGenerator for BinaryGenerator {
    fn generate(&self, rng: &mut ThreadRng, n: usize) -> Result<Arc<dyn Array>> {
        let mut builder = BinaryBuilder::new(n);
        for i in 0..n {
            if i % 5 == 0 {
                builder.append_null()?;
            } else {
                let len = rng.gen_range(5..20); // Random length between 5 and 20
                let mut bytes = vec![0u8; len];
                rng.fill_bytes(&mut bytes);
                builder.append_value(&bytes)?;
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}

struct Date32Generator {}

impl ArrayGenerator for Date32Generator {
    fn generate(&self, rng: &mut ThreadRng, n: usize) -> Result<Arc<dyn Array>> {
        let mut builder = Date32Builder::new(n);
        for i in 0..n {
            if i % 5 == 0 {
                builder.append_null()?;
            } else {
                // Generate a random date represented as the number of days since 1970-01-01
                let days_since_epoch = rng.gen_range(0..=18262); // Roughly 50 years range
                builder.append_value(days_since_epoch)?;
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}

struct Time32MillisecondGenerator {}

impl ArrayGenerator for Time32MillisecondGenerator {
    fn generate(&self, rng: &mut ThreadRng, n: usize) -> Result<Arc<dyn Array>> {
        let mut builder = Time32MillisecondBuilder::new(n);
        for i in 0..n {
            if i % 5 == 0 {
                builder.append_null()?;
            } else {
                // Time in milliseconds since midnight (0 - 86_399_999)
                builder.append_value(rng.gen_range(0..86400000))?;
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}

struct TimestampMillisecondGenerator {}

impl ArrayGenerator for TimestampMillisecondGenerator {
    fn generate(&self, rng: &mut ThreadRng, n: usize) -> Result<Arc<dyn Array>> {
        let mut builder = TimestampMillisecondBuilder::new(n);
        for i in 0..n {
            if i % 5 == 0 {
                builder.append_null()?;
            } else {
                // Generate a random timestamp in milliseconds
                // Assuming a range of about 50 years
                let timestamp_ms = rng.gen_range(0..1_576_800_000_000i64); // Roughly 50 years in milliseconds
                builder.append_value(timestamp_ms)?;
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}

struct DecimalGenerator {
    precision: usize,
    scale: usize,
}

impl ArrayGenerator for DecimalGenerator {
    fn generate(&self, rng: &mut ThreadRng, n: usize) -> Result<Arc<dyn Array>> {
        let mut builder = DecimalBuilder::new(n, self.precision, self.scale);
        for i in 0..n {
            if i % 5 == 0 {
                builder.append_null()?;
            } else {
                // Generate a random Decimal value based on precision and scale
                let max_value = Decimal::new(10i64.pow(self.precision as u32) - 1, 0);
                let mut value = Decimal::new(rng.gen_range(0..=max_value.to_i64().unwrap()), 0);
                value.set_scale(self.scale as u32).unwrap();
                builder.append_value(i128::try_from(value).unwrap())?;
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}
