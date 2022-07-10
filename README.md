# SQL Fuzz Testing Utilities

Generate random (and sometimes valid!) SQL queries from any local datasets in Parquet, CSV, JSON, or Avro format.

## Installation

```bash
cargo install sqlfuzz
```

## Query Fuzzing

```bash
sqlfuzz query \
  --table ./testdata/test0.parquet ./testdata/test1.parquet \
  --join-type inner left right full semi anti \
  --count 5 \
  --max-depth 5
```

### Example Generated Query

```sql
SELECT table_alias_29.a, table_alias_29.b, table_alias_29.c
FROM
    (SELECT table_alias_28.a, table_alias_28.b, table_alias_28.c
     FROM (SELECT table_alias_27.a, table_alias_27.b, table_alias_27.c
           FROM (SELECT test.a, test.b, test.c
                 FROM test) table_alias_27
           WHERE table_alias_27.a <= table_alias_27.a) table_alias_28) table_alias_29
        Full JOIN
    (SELECT table_alias_31.a, table_alias_31.b, table_alias_31.c
     FROM (SELECT table_alias_30.a, table_alias_30.b, table_alias_30.c
           FROM (SELECT test.a, test.b, test.c
                 FROM test) table_alias_30) table_alias_31) table_alias_32
    ON table_alias_29.b = table_alias_32.a;
```

## Data Generator

If you don't already have suitable data, you can generate random data files to run the query fuzzer against.

```bash
sqlfuzz data --path ./testdata --num-files 4 --row-count 20
```