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

use datafusion::arrow::array::{Array, Int32Array, Int8Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
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
use std::io::{BufRead, BufReader};
use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};
use rand::prelude::ThreadRng;
use rand::Rng;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "sqlfuzz", about = "SQLFuzz: Query Engine Fuzz Testing")]
enum Config {
    /// Generate random queries
    Query(QueryGen),
    /// Generate random data files
    Data(DataGen),
    /// Run SQL queries and capture results
    Execute(ExecuteConfig),
    /// Compare two test runs
    Compare(CompareConfig),
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

#[derive(Debug, StructOpt)]
struct CompareConfig {
    #[structopt(required = true)]
    report1: PathBuf,
    #[structopt(required = true)]
    report2: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    match Config::from_args() {
        Config::Query(config) => query_gen(&config).await,
        Config::Data(config) => data_gen(&config).await,
        Config::Execute(config) => execute(&config).await,
        Config::Compare(config) => compare(&config).await,
    }
}

async fn compare(config: &CompareConfig) -> Result<()> {
    let report1 = read_report(&config.report1)?;
    let report2 = read_report(&config.report2)?;
    assert_eq!(report1.results.len(), report2.results.len());
    for i in 0..report1.results.len() {
        let mut result1 = report1.results[i].rows.clone();
        result1.sort();
        let mut result2 = report2.results[i].rows.clone();
        result2.sort();
        println!("COMPARE");
        println!("{:?}", result1);
        println!("WITH");
        println!("{:?}", result2);
        if result1 == result2 {
            println!("VERDICT: SAME");
        } else {
            println!("VERDICT: DIFFERENT");
        }
        println!("-------------------------");
    }
    Ok(())
}

struct ResultSet {
    rows: Vec<Vec<String>>,
}

struct Report {
    results: Vec<ResultSet>,
}

fn read_report(filename: &PathBuf) -> Result<Report> {
    let file = File::open(filename)?;
    let reader = BufReader::new(file);
    let lines = reader.lines();
    let mut report = Report { results: vec![] };
    let mut rows = vec![];
    let mut in_result = true;
    for line in lines {
        let line = line?;
        if line.starts_with("-- BEGIN RESULT --") {
            in_result = true;
        } else if line.starts_with("-- END RESULT --") {
            in_result = false;
            report.results.push(ResultSet { rows });
            rows = vec![];
        } else {
            if in_result {
                rows.push(line.split('\t').map(|s| s.to_string()).collect());
            }
        }
    }
    Ok(report)
}

async fn execute(config: &ExecuteConfig) -> Result<()> {
    // register tables with context
    let (ctx, _) = create_datafusion_context(&config.table, config.verbose).await?;

    let file = File::open(&config.sql)?;
    let reader = BufReader::new(file);
    let lines = reader.lines();
    let mut sql = String::new();
    for line in lines {
        let line = line?;
        if line.starts_with("--") {
            println!("{}", line);
        } else {
            sql.push_str(&line);
            sql.push('\n');
            if sql.trim().ends_with(';') {
                println!("{}", sql);
                println!("-- BEGIN RESULT --");
                match ctx.sql(&sql).await {
                    Ok(df) => match df.collect().await {
                        Ok(batches) => {
                            print_results(batches);
                        }
                        Err(e) => {
                            println!("{:?}", e);
                        }
                    },
                    Err(e) => {
                        println!("{:?}", e);
                    }
                }
                println!("-- END RESULT --");
                sql = String::new();
            }
        }
    }
    Ok(())
}

fn print_results(batches: Vec<RecordBatch>) {
    for batch in &batches {
        for i in 0..batch.num_rows() {
            let mut csv = String::new();
            for j in 0..batch.num_columns() {
                if j > 0 {
                    csv.push('\t');
                }
                let col = batch.column(j);
                match col.data_type() {
                    DataType::Int8 => {
                        let col = col.as_any().downcast_ref::<Int8Array>().unwrap();
                        if col.is_null(i) {
                            csv.push_str("null");
                        } else {
                            csv.push_str(&format!("{}", col.value(i)))
                        }
                    }
                    DataType::Int32 => {
                        let col = col.as_any().downcast_ref::<Int32Array>().unwrap();
                        if col.is_null(i) {
                            csv.push_str("null");
                        } else {
                            csv.push_str(&format!("{}", col.value(i)))
                        }
                    }
                    DataType::Utf8 => {
                        let col = col.as_any().downcast_ref::<StringArray>().unwrap();
                        if col.is_null(i) {
                            csv.push_str("null");
                        } else {
                            csv.push_str(&format!("{}", col.value(i)))
                        }
                    }
                    _ => unimplemented!(),
                }
            }
            println!("{}", csv);
        }
    }
}

fn random_data_type(rng: &mut ThreadRng) -> DataType {
    let types = vec![
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::Binary,
        DataType::Date32,
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Timestamp(TimeUnit::Millisecond, None),
        // Decimal types with different scales
        DataType::Decimal(9, 2),
        DataType::Decimal(20, 4),
        DataType::Decimal(38, 6),
        DataType::Decimal(45, 8),
    ];

    types[rng.gen_range(0..types.len())].clone()
}

fn random_schema(rng: &mut ThreadRng, num_fields: usize) -> Schema {
    let mut fields = Vec::new();
    for i in 0..num_fields {
        let data_type = random_data_type(rng);
        fields.push(Field::new(&format!("c{}", i), data_type, true));
    }
    Schema::new(fields)
}

async fn data_gen(config: &DataGen) -> Result<()> {
    let mut rng = rand::thread_rng();
    let schema = Arc::new(random_schema(&mut rng, 9));

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

// TODO: generate sql dml statements to populate tables
fn create_table_statement(schema: &Schema, table_name: &str) -> String {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let column_type = match field.data_type() {
                DataType::Boolean => "BOOLEAN".to_string(),
                DataType::Int8 => "TINYINT".to_string(),
                DataType::Int16 => "SMALLINT".to_string(),
                DataType::Int32 => "INT".to_string(),
                DataType::Int64 => "BIGINT".to_string(),
                DataType::Float32 => "FLOAT".to_string(),
                DataType::Float64 => "DOUBLE".to_string(),
                DataType::Utf8 => "STRING".to_string(),
                DataType::Decimal(precision, scale) => format!("DECIMAL({}, {})", precision, scale),
                // TODO: more data types
                _ => unimplemented!(),
            };
            format!("{} {}", field.name(), column_type)
        })
        .collect::<Vec<String>>()
        .join(", ");

    format!("CREATE TABLE {} ({});", table_name, columns)
}

fn create_insert_statements(batch: &RecordBatch, table_name: &str) -> Vec<String> {
    (0..batch.num_rows()).map(|row_index| {
        let values = batch
            .columns()
            .iter()
            .map(|column| {
                // Serialize each value to a SQL-friendly format
                if column.is_null(row_index) {
                    "NULL".to_string()
                } else {
                    match column.data_type() {
                        DataType::Utf8 => format!("'{}'", column.as_any().downcast_ref::<StringArray>().unwrap().value(row_index)),
                        DataType::Int32 => format!("{}", column.as_any().downcast_ref::<Int32Array>().unwrap().value(row_index)),
                        // TODO: Add cases for other data types
                        _ => unimplemented!(),
                    }
                }
            })
            .collect::<Vec<String>>()
            .join(", ");

        format!("INSERT INTO {} VALUES ({});", table_name, values)
    }).collect()
}

async fn data_gen_sql(config: &DataGen, table_name: &str) -> Result<()> {
    let mut rng = rand::thread_rng();
    let schema = Arc::new(random_schema(&mut rng, 9));

    let table_statement = create_table_statement(&schema, table_name);
    println!("{}", table_statement); // Output the table creation statement

    let batch = generate_batch(&mut rng, &schema, config.row_count)?;
    let insert_statements = create_insert_statements(&batch, table_name);

    // Output the insert statements
    for statement in insert_statements {
        println!("{}", statement);
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

async fn create_datafusion_context(
    table: &[PathBuf],
    verbose: bool,
) -> Result<(SessionContext, Vec<SQLTable>)> {
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
