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

If you don't already have suitable data, you can generate random data files to run the query fuzzer against.

```bash
sqlfuzz data --path ./testdata --num-files 4 --row-count 20
```