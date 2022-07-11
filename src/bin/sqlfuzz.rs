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

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::logical_plan::JoinType;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::{
    common::{DataFusionError, Result},
    dataframe::DataFrame,
    prelude::{
        AvroReadOptions, CsvReadOptions, NdJsonReadOptions, ParquetReadOptions, SessionContext,
    },
};
use sqlfuzz::{generate_batch, plan_to_sql, FuzzConfig, SQLRelationGenerator, SQLTable};
use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};
use std::io::{BufRead, BufReader};
use datafusion::arrow::array::{Array, Int32Array, StringArray};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "sqlfuzz", about = "sqlfuzz SQL query generator")]
enum Config {
    /// Generate random queries
    Query(QueryGen),
    /// Generate random data files
    Data(DataGen),
    /// Run SQL queries and capture results
    Execute(ExecuteConfig),
}

#[derive(Debug, StructOpt)]
struct DataGen {
    #[structopt(short, long, required = true)]
    num_files: usize,
    #[structopt(short, long, required = true)]
    path: PathBuf,
    #[structopt(short, long, default_value = "20")]
    row_count: usize,
}

#[derive(Debug, StructOpt)]
struct QueryGen {
    #[structopt(parse(from_os_str), long, required = true, multiple = true)]
    table: Vec<PathBuf>,
    #[structopt(short, long, required = false, multiple = true)]
    join_type: Vec<String>,
    #[structopt(short, long, default_value = "10")]
    count: usize,
    #[structopt(short, long, default_value = "5")]
    max_depth: usize,
    #[structopt(short, long)]
    verbose: bool,
}

#[derive(Debug, StructOpt)]
struct ExecuteConfig {
    #[structopt(parse(from_os_str), long, required = true, multiple = true)]
    table: Vec<PathBuf>,
    #[structopt(short, long, required = true)]
    sql: PathBuf,
    #[structopt(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    match Config::from_args() {
        Config::Query(config) => query_gen(&config).await,
        Config::Data(config) => data_gen(&config).await,
        Config::Execute(config) => execute(&config).await,
    }
}

async fn execute(config: &ExecuteConfig) -> Result<()> {
    // register tables with context
    let (ctx, _) = create_datafusion_context(&config.table, config.verbose).await?;

    let file = File::open(&config.sql)?;
    let reader = BufReader::new(file);
    let lines = reader.lines();
    let mut sql= String::new();
    for line in lines {
        let line = line?;
        if !line.starts_with("--") {
            sql.push_str(&line);
            sql.push('\n');
            if sql.trim().ends_with(';') {
                println!("{}", sql);
                let df = ctx.sql(&sql).await?;
                let batches = df.collect().await?;
                for batch in &batches {
                    for i in 0..batch.num_rows() {
                        let mut csv = String::new();
                        for j in 0..batch.num_columns() {
                            if j > 0 {
                                csv.push(',');
                            }
                            let col = batch.column(j);
                            match col.data_type() {
                                DataType::Int32 => {
                                    let col = col.as_any().downcast_ref::<Int32Array>().unwrap();
                                    // TODO handle nulls
                                    csv.push_str(&format!("{}", col.value(i)))
                                }
                                DataType::Utf8 => {
                                    let col = col.as_any().downcast_ref::<StringArray>().unwrap();
                                    // TODO handle nulls
                                    csv.push_str(&format!("{}", col.value(i)))
                                }
                                _ => unimplemented!()
                            }
                        }
                        println!("{}", csv);
                    }
                }
                sql = String::new();
            }
        }
    }
    Ok(())
}

async fn data_gen(config: &DataGen) -> Result<()> {
    //TODO randomize the schema and support more types
    let schema = Arc::new(Schema::new(vec![
        Field::new("c0", DataType::Int32, true),
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Int32, true),
        Field::new("c3", DataType::Utf8, true),
    ]));

    let mut rng = rand::thread_rng();
    let writer_properties = WriterProperties::builder().build();

    for i in 0..config.num_files {
        let batch = generate_batch(&mut rng, &schema, config.row_count)?;
        let filename = format!("test{}.parquet", i);
        let path = config.path.join(&filename);
        println!("Generating {:?}", path);
        let file = File::create(path)?;
        let mut writer =
            ArrowWriter::try_new(file, schema.clone(), Some(writer_properties.clone()))?;
        writer.write(&batch)?;
        writer.close()?;
    }
    Ok(())
}

async fn query_gen(config: &QueryGen) -> Result<()> {
    if config.table.is_empty() {
        panic!("must provide tables to generate queries for");
    }

    let mut join_types = vec![];
    for jt in &config.join_type {
        let jt = match jt.as_str() {
            "anti" => JoinType::Anti,
            "semi" => JoinType::Semi,
            "left" => JoinType::Left,
            "right" => JoinType::Right,
            "full" => JoinType::Full,
            "inner" => JoinType::Inner,
            other => panic!("invalid join type: {}", other),
        };
        join_types.push(jt);
    }

    // register tables with context
    let (ctx, sql_tables) = create_datafusion_context(&config.table, config.verbose).await?;

    // generate a random SQL query
    let num_queries = config.count;
    let mut rng = rand::thread_rng();

    let fuzz_config = FuzzConfig {
        join_types,
        max_depth: config.max_depth,
    };

    let mut gen = SQLRelationGenerator::new(&mut rng, sql_tables, fuzz_config);

    let mut generated = 0;

    while generated < num_queries {
        let plan = gen.generate_select()?;
        if config.verbose {
            let logical_plan = plan.to_logical_plan();
            println!("Input plan:\n{:?}", logical_plan);
        }
        let sql = plan_to_sql(&plan, 0)?;

        // see if we produced something valid or not (according to DataFusion's
        // SQL query planner)
        match ctx.create_logical_plan(&sql) {
            Ok(_plan) => {
                generated += 1;
                println!("-- SQL Query #{}:\n\n{};\n\n", generated, sql);
                // println!("Plan:\n\n{:?}", plan)
            }
            Err(e) if config.verbose => {
                println!("-- SQL:\n\n{};\n\n", sql);
                println!("-- SQL was not valid: {:?}\n\n", e)
            }
            _ => {
                // ignore
            }
        }
    }

    Ok(())
}

async fn create_datafusion_context(table: &[PathBuf], verbose: bool) -> Result<(SessionContext, Vec<SQLTable>)> {
    let ctx = SessionContext::new();
    let mut sql_tables: Vec<SQLTable> = vec![];
    for path in table {
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
    Ok((ctx, sql_tables))
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
