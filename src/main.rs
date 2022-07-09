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
    common::{Column, DFField, DFSchema, DFSchemaRef, DataFusionError, Result},
    dataframe::DataFrame,
    datasource::{DefaultTableSource, TableProvider, TableType},
    execution::context::SessionState,
    logical_expr::{
        logical_plan::{
            Filter, Join, JoinConstraint, JoinType, Projection, SubqueryAlias, TableScan,
        },
        Expr, LogicalPlan, Operator,
    },
    physical_plan::ExecutionPlan,
    prelude::{
        AvroReadOptions, CsvReadOptions, NdJsonReadOptions, ParquetReadOptions, SessionContext,
    },
};
use rand::{rngs::ThreadRng, Rng};
use std::{
    any::Any,
    path::{Path, PathBuf},
    sync::Arc,
};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "sqlfuzz", about = "SQL Fuzz Testing Tool")]
enum Command {
    Generate {
        #[structopt(parse(from_os_str), long)]
        table: Vec<PathBuf>,
        #[structopt(short, long)]
        count: Option<usize>,
        #[structopt(short, long)]
        max_depth: Option<usize>,
        #[structopt(short, long)]
        verbose: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cmd = Command::from_args();
    match cmd {
        Command::Generate {
            table,
            verbose,
            count,
            max_depth,
        } => {
            if table.is_empty() {
                panic!("must provide tables to generate queries for");
            }

            // register tables with context
            let ctx = SessionContext::new();
            let mut sql_tables: Vec<SQLTable> = vec![];
            for path in &table {
                let table_name = path
                    .file_stem()
                    .unwrap()
                    .to_str()
                    .ok_or_else(|| DataFusionError::Internal("Invalid filename".to_string()))?;
                let table_name = sanitize_table_name(table_name);
                let filename = parse_filename(path)?;
                if verbose {
                    println!("Registering table '{}' for {}", table_name, path.display());
                }
                let df = register_table(&ctx, &table_name, filename).await?;
                sql_tables.push(SQLTable::new(&table_name, df.schema().clone()));
            }

            // generate a random SQL query
            let num_queries = count.unwrap_or(10);
            let mut rng = rand::thread_rng();
            let mut gen = SQLRelationGenerator::new(&mut rng, sql_tables, max_depth.unwrap_or(3));

            for _ in 0..num_queries {
                let plan = gen.generate_relation()?;
                // always wrap in a final projection
                let plan = match plan {
                    SQLRelation::Select { .. } => plan.clone(),
                    _ => gen.select_star(&plan),
                };
                if verbose {
                    let logical_plan = plan.to_logical_plan();
                    println!("Input plan:\n{:?}", logical_plan);
                }
                let sql = plan_to_sql(&plan)?;

                // see if we produced something valid or not (according to DataFusion's
                // SQL query planner)
                match ctx.create_logical_plan(&sql) {
                    Ok(plan) => {
                        println!("SQL:\n\n{};\n\n", sql);
                        println!("Plan:\n\n{:?}", plan)
                    }
                    Err(e) if verbose => {
                        println!("SQL: {};\n\n", sql);
                        println!("SQL was not valid: {:?}", e)
                    }
                    _ => {
                        // ignore
                    }
                }
            }

            Ok(())
        }
    }
}

fn parse_filename(filename: &Path) -> Result<&str> {
    filename
        .to_str()
        .ok_or_else(|| DataFusionError::Internal("Invalid filename".to_string()))
}

enum FileFormat {
    Avro,
    Csv,
    Json,
    Parquet,
}

fn file_format(filename: &str) -> Result<FileFormat> {
    match filename.rfind('.') {
        Some(i) => match &filename[i + 1..] {
            "avro" => Ok(FileFormat::Avro),
            "csv" => Ok(FileFormat::Csv),
            "json" => Ok(FileFormat::Json),
            "parquet" => Ok(FileFormat::Parquet),
            other => Err(DataFusionError::Internal(format!(
                "unsupported file extension '{}'",
                other
            ))),
        },
        _ => Err(DataFusionError::Internal(format!(
            "Could not determine file extension for '{}'",
            filename
        ))),
    }
}

fn sanitize_table_name(name: &str) -> String {
    let mut str = String::new();
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            str.push(ch);
        } else {
            str.push('_')
        }
    }
    str
}

async fn register_table(
    ctx: &SessionContext,
    table_name: &str,
    filename: &str,
) -> Result<Arc<DataFrame>> {
    match file_format(filename)? {
        FileFormat::Avro => {
            ctx.register_avro(table_name, filename, AvroReadOptions::default())
                .await?
        }
        FileFormat::Csv => {
            ctx.register_csv(table_name, filename, CsvReadOptions::default())
                .await?
        }
        FileFormat::Json => {
            ctx.register_json(table_name, filename, NdJsonReadOptions::default())
                .await?
        }
        FileFormat::Parquet => {
            ctx.register_parquet(table_name, filename, ParquetReadOptions::default())
                .await?
        }
    }
    ctx.table(table_name)
}

struct SQLTable {
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
enum SQLRelation {
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
        self.to_logical_plan().schema().clone()
    }

    pub fn to_logical_plan(&self) -> LogicalPlan {
        match self {
            Self::Select {
                projection,
                filter,
                input,
            } => {
                let projection = LogicalPlan::Projection(Projection {
                    expr: projection.clone(),
                    input: Arc::new(input.to_logical_plan().clone()),
                    schema: input.schema().clone(), // TODO this assume the project is SELECT *
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
                left: Arc::new(left.to_logical_plan()),
                right: Arc::new(right.to_logical_plan()),
                on: on.clone(),
                filter: filter.clone(),
                join_type: join_type.clone(),
                join_constraint: join_constraint.clone(),
                schema: schema.clone(),
                null_equals_null: false,
            }),
            Self::TableScan(x) => LogicalPlan::TableScan(x.clone()),
            Self::SubqueryAlias {
                input,
                alias,
                schema,
            } => LogicalPlan::SubqueryAlias(SubqueryAlias {
                input: Arc::new(input.to_logical_plan().clone()),
                alias: alias.clone(),
                schema: schema.clone(),
            }),
        }
    }
}

/// Generates random logical plans
struct SQLRelationGenerator<'a> {
    tables: Vec<SQLTable>,
    rng: &'a mut ThreadRng,
    id_gen: usize,
    depth: usize,
    max_depth: usize,
}

impl<'a> SQLRelationGenerator<'a> {
    fn new(rng: &'a mut ThreadRng, tables: Vec<SQLTable>, max_depth: usize) -> Self {
        Self {
            tables,
            rng,
            id_gen: 0,
            depth: 0,
            max_depth,
        }
    }

    fn generate_select(&mut self) -> Result<SQLRelation> {
        let input = self.generate_relation()?;
        let input = self.alias(&input);
        let projection = input
            .to_logical_plan()
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

    fn generate_relation(&mut self) -> Result<SQLRelation> {
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

    fn select_star(&mut self, plan: &SQLRelation) -> SQLRelation {
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
            left: Box::new(t1.clone()),
            right: Box::new(t2.clone()),
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

fn plan_to_sql(plan: &SQLRelation) -> Result<String> {
    match plan {
        SQLRelation::Select {
            projection,
            filter,
            input,
        } => {
            let expr: Vec<String> = projection.iter().map(expr_to_sql).collect();
            let input = plan_to_sql(input)?;
            let where_clause = if let Some(predicate) = filter {
                format!(" WHERE {}", expr_to_sql(predicate))
            } else {
                "".to_string()
            };
            Ok(format!(
                "SELECT {} FROM {}{}",
                expr.join(", "),
                input,
                where_clause
            ))
        }
        SQLRelation::TableScan(scan) => Ok(scan.table_name.clone()),
        SQLRelation::Join {
            left,
            right,
            on,
            join_type,
            ..
        } => {
            let l = plan_to_sql(left)?;
            let r = plan_to_sql(right)?;
            let join_condition = on
                .iter()
                .map(|(l, r)| format!("{} = {}", l.flat_name(), r.flat_name()))
                .collect::<Vec<_>>()
                .join(" AND ");
            Ok(format!(
                "{} {} JOIN {} ON {}",
                l, join_type, r, join_condition
            ))
        }
        SQLRelation::SubqueryAlias { input, alias, .. } => {
            Ok(format!("({}) {}", plan_to_sql(input)?, alias))
        }
    }
}

fn expr_to_sql(expr: &Expr) -> String {
    match expr {
        Expr::Column(col) => col.flat_name(),
        Expr::BinaryExpr { left, op, right } => {
            format!("{} {} {}", expr_to_sql(left), op, expr_to_sql(right))
        }
        other => other.to_string(),
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
