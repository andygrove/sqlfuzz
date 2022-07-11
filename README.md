# SQL Fuzz Testing Utilities

`sqlfuzz` is a command-line tool and library for generating random data files and SQL queries.

For background information on SQL fuzzing, I recommend reading the paper [SparkFuzz: Searching Correctness Regressions in
Modern Query Engines](https://ir.cwi.nl/pub/30222/3395032.3395327.pdf).

## Status

- [x] Generate random SQL queries against any provided input files
- [x] Generate randomized data files
- [ ] Provide tooling to execute queries and capture results
  - [ ] DataFusion
  - [ ] Apache Spark
  - [ ] PostgreSQL
- [ ] Provide tooling to compare two sets of results and report differences

## Installation

```bash
cargo install sqlfuzz
```

## SQL Query Fuzzing

Use the following syntax to generate randomized SQL queries against the example data files in this repository.

```bash
sqlfuzz query \
  --table ./testdata/test0.parquet ./testdata/test1.parquet \
  --join-type inner left right full semi anti \
  --count 5 \
  --max-depth 5
```

### Example Generated Query

```sql
SELECT __c320, __c321, __c322, __c323
FROM (
    (SELECT test1.c0 AS __c320, test1.c1 AS __c321, test1.c2 AS __c322, test1.c3 AS __c323
    FROM (test1))
    INNER JOIN
    (SELECT test1.c0 AS __c324, test1.c1 AS __c325, test1.c2 AS __c326, test1.c3 AS __c327
    FROM (test1))
    ON __c320 = __c327)
WHERE __c323 > __c320;

```

## Data Generator

`sqlfuzz` can generate random data files to run the query fuzzer against. The files are generated in Parquet format.

```bash
sqlfuzz data --path ./testdata --num-files 4 --row-count 256
```

## Executing Queries

```bash
sqlfuzz execute \
  --table ./testdata/test0.parquet ./testdata/test1.parquet \
  --sql testdata/example.sql
```

## Comparing Results

```bash
sqlfuzz compare report1.txt report2.txt
```