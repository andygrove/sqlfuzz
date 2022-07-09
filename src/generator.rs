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

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{Column, DFField, DFSchema, DFSchemaRef, Result},
    datasource::{DefaultTableSource, TableProvider, TableType},
    execution::context::SessionState,
    logical_expr::{
        logical_plan::{
            Filter, Join, JoinConstraint, JoinType, Projection, SubqueryAlias, TableScan,
        },
        Expr, ExprSchemable, LogicalPlan, Operator,
    },
    physical_plan::ExecutionPlan,
};
use rand::rngs::ThreadRng;
use rand::Rng;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

pub struct SQLTable {
    name: String,
    schema: DFSchema,
}

impl SQLTable {
    pub fn new(name: &str, schema: DFSchema) -> Self {
        Self {
            name: name.to_owned(),
            schema,
        }
    }
}

#[derive(Clone)]
pub enum SQLRelation {
    Select {
        projection: Vec<Expr>,
        filter: Option<Expr>,
        input: Box<SQLRelation>,
    },
    Join {
        left: Box<SQLRelation>,
        right: Box<SQLRelation>,
        on: Vec<(Column, Column)>,
        filter: Option<Expr>,
        join_type: JoinType,
        join_constraint: JoinConstraint,
        schema: DFSchemaRef,
    },
    TableScan(TableScan),
    SubqueryAlias {
        input: Box<SQLRelation>,
        alias: String,
        schema: DFSchemaRef,
    },
}

impl SQLRelation {
    pub fn schema(&self) -> DFSchemaRef {
        self.to_logical_plan().unwrap().schema().clone()
    }

    pub fn to_logical_plan(&self) -> Result<LogicalPlan> {
        Ok(match self {
            Self::Select {
                projection,
                filter,
                input,
            } => {
                let input = Arc::new(input.to_logical_plan()?);
                let input_schema = input.schema();

                let fields = projection
                    .iter()
                    .map(|e| e.to_field(input_schema))
                    .collect::<Result<Vec<_>>>()?;
                let schema = Arc::new(DFSchema::new_with_metadata(fields, HashMap::new())?);

                let projection = LogicalPlan::Projection(Projection {
                    expr: projection.clone(),
                    input: input,
                    schema,
                    alias: None,
                });
                if let Some(predicate) = filter {
                    LogicalPlan::Filter(Filter {
                        predicate: predicate.clone(),
                        input: Arc::new(projection),
                    })
                } else {
                    projection
                }
            }
            Self::Join {
                left,
                right,
                on,
                filter,
                join_type,
                join_constraint,
                schema,
            } => LogicalPlan::Join(Join {
                left: Arc::new(left.to_logical_plan()?),
                right: Arc::new(right.to_logical_plan()?),
                on: on.clone(),
                filter: filter.clone(),
                join_type: *join_type,
                join_constraint: *join_constraint,
                schema: schema.clone(),
                null_equals_null: false,
            }),
            Self::TableScan(x) => LogicalPlan::TableScan(x.clone()),
            Self::SubqueryAlias {
                input,
                alias,
                schema,
            } => LogicalPlan::SubqueryAlias(SubqueryAlias {
                input: Arc::new(input.to_logical_plan()?),
                alias: alias.clone(),
                schema: schema.clone(),
            }),
        })
    }
}

/// Generates random logical plans
pub struct SQLRelationGenerator<'a> {
    tables: Vec<SQLTable>,
    rng: &'a mut ThreadRng,
    id_gen: usize,
    depth: usize,
    max_depth: usize,
}

impl<'a> SQLRelationGenerator<'a> {
    pub fn new(rng: &'a mut ThreadRng, tables: Vec<SQLTable>, max_depth: usize) -> Self {
        Self {
            tables,
            rng,
            id_gen: 0,
            depth: 0,
            max_depth,
        }
    }

    pub fn generate_select(&mut self) -> Result<SQLRelation> {
        let input = self.generate_relation()?;
        let input = self.alias(&input);
        let projection = input
            .to_logical_plan()?
            .schema()
            .fields()
            .iter()
            .map(|f| Expr::Column(f.qualified_column()))
            .collect();
        let filter = match self.rng.gen_range(0..2) {
            0 => Some(self.generate_predicate(&input)?),
            1 => None,
            _ => unreachable!(),
        };
        Ok(SQLRelation::Select {
            projection,
            filter,
            input: Box::new(input.clone()),
        })
    }

    fn generate_predicate(&mut self, input: &SQLRelation) -> Result<Expr> {
        let l = self.get_random_field(input.schema().as_ref());
        let r = self.get_random_field(input.schema().as_ref());
        let op = match self.rng.gen_range(0..6) {
            0 => Operator::Lt,
            1 => Operator::LtEq,
            2 => Operator::Gt,
            3 => Operator::GtEq,
            4 => Operator::Eq,
            5 => Operator::NotEq,
            _ => unreachable!(),
        };
        Ok(Expr::BinaryExpr {
            left: Box::new(Expr::Column(l.qualified_column())),
            op,
            right: Box::new(Expr::Column(r.qualified_column())),
        })
    }

    fn get_random_field(&mut self, schema: &DFSchema) -> DFField {
        let i = self.rng.gen_range(0..schema.fields().len());
        let field = schema.field(i);
        field.clone()
    }

    pub fn generate_relation(&mut self) -> Result<SQLRelation> {
        if self.depth == self.max_depth {
            // generate a leaf node to prevent us recursing forever
            self.generate_table_scan()
        } else {
            self.depth += 1;
            let plan = match self.rng.gen_range(0..3) {
                0 => self.generate_table_scan(),
                1 => self.generate_join(),
                2 => self.generate_select(),
                _ => unreachable!(),
            };
            self.depth -= 1;
            plan
        }
    }

    fn generate_alias(&mut self) -> String {
        let id = self.id_gen;
        self.id_gen += 1;
        format!("table_alias_{}", id)
    }

    pub fn select_star(&mut self, plan: &SQLRelation) -> SQLRelation {
        let fields = plan.schema().fields().clone();
        let projection = fields
            .iter()
            .map(|f| Expr::Column(f.qualified_column()))
            .collect();
        SQLRelation::Select {
            projection,
            filter: None,
            input: Box::new(plan.clone()),
        }
    }

    fn alias(&mut self, plan: &SQLRelation) -> SQLRelation {
        match plan {
            SQLRelation::SubqueryAlias { .. } => plan.clone(),
            _ => {
                let alias = self.generate_alias();
                let schema = plan.schema().as_ref().clone();
                let schema = schema.replace_qualifier(&alias);
                SQLRelation::SubqueryAlias {
                    input: Box::new(plan.clone()),
                    alias,
                    schema: Arc::new(schema),
                }
            }
        }
    }

    fn generate_join(&mut self) -> Result<SQLRelation> {
        let t1 = self.generate_relation()?;
        let t1 = self.alias(&t1);
        let t2 = self.generate_relation()?;
        let t2 = self.alias(&t2);
        let schema = t1.schema().as_ref().clone();
        schema.join(&t2.schema())?;

        // TODO generate multicolumn joins
        let on = vec![(
            t1.schema().fields()[self.rng.gen_range(0..t1.schema().fields().len())]
                .qualified_column(),
            t2.schema().fields()[self.rng.gen_range(0..t2.schema().fields().len())]
                .qualified_column(),
        )];

        let join_type = match self.rng.gen_range(0..4) {
            0 => JoinType::Inner,
            1 => JoinType::Left,
            2 => JoinType::Right,
            3 => JoinType::Full,
            _ => unreachable!(),
        };

        Ok(SQLRelation::Join {
            left: Box::new(t1),
            right: Box::new(t2),
            on,
            filter: None,
            join_type,
            join_constraint: JoinConstraint::On,
            schema: Arc::new(schema),
        })
    }

    fn generate_table_scan(&mut self) -> Result<SQLRelation> {
        let table = self.random_table();
        let projected_schema = Arc::new(table.schema.clone());
        let table_scan = SQLRelation::TableScan(TableScan {
            table_name: table.name.clone(),
            source: Arc::new(DefaultTableSource {
                table_provider: Arc::new(FakeTableProvider {}),
            }),
            projection: None,
            projected_schema,
            filters: vec![],
            fetch: None,
        });
        let projection = self.select_star(&table_scan);
        Ok(self.alias(&projection))
    }

    fn random_table(&mut self) -> &SQLTable {
        &self.tables[self.rng.gen_range(0..self.tables.len())]
    }
}

struct FakeTableProvider {}

#[async_trait]
impl TableProvider for FakeTableProvider {
    fn as_any(&self) -> &dyn Any {
        unreachable!()
    }

    fn schema(&self) -> SchemaRef {
        unreachable!()
    }

    fn table_type(&self) -> TableType {
        unreachable!()
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        _projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unreachable!()
    }
}

#[cfg(test)]
mod test {
    use crate::{SQLRelationGenerator, SQLTable};
    use datafusion::{
        arrow::datatypes::DataType,
        common::{DFField, DFSchema, Result},
    };
    use std::collections::HashMap;

    #[test]
    fn test() -> Result<()> {
        let mut rng = rand::thread_rng();
        let mut gen = SQLRelationGenerator::new(&mut rng, test_tables()?);
        let _plan = gen.generate_relation()?;
        Ok(())
    }

    fn test_tables() -> Result<Vec<SQLTable>> {
        Ok(vec![SQLTable::new(
            "foo",
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(None, "a", DataType::Int32, true),
                    DFField::new(None, "b", DataType::Utf8, true),
                ],
                HashMap::new(),
            )?,
        )])
    }
}
