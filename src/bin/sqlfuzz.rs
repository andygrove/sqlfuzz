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

use datafusion::{
    common::{DataFusionError, Result},
    dataframe::DataFrame,
    prelude::{
        AvroReadOptions, CsvReadOptions, NdJsonReadOptions, ParquetReadOptions, SessionContext,
    },
};
use sqlfuzz::{plan_to_sql, SQLRelation, SQLRelationGenerator, SQLTable};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "sqlfuzz", about = "sqlfuzz SQL query generator")]
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
