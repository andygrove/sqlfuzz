# SQL Fuzz Testing Utilities

Generate random (and sometimes valid!) SQL queries from any local datasets in Parquet, CSV, JSON, or Avro format.

```bash
$ cargo run -- generate --table ./testdata/test.csv

SQL:

SELECT table_alias_25.a, table_alias_25.b, table_alias_25.c 
FROM (SELECT test.a, test.b, test.c FROM test) table_alias_25 
Full JOIN 
(
  SELECT table_alias_26.a, table_alias_26.b, table_alias_26.c 
  FROM 
  (SELECT test.a, test.b, test.c FROM test) table_alias_26 
  WHERE table_alias_26.c < table_alias_26.c
) table_alias_27 
ON table_alias_25.a = table_alias_27.c

Plan:

Projection: #table_alias_25.a, #table_alias_25.b, #table_alias_25.c
  Full Join: #table_alias_25.a = #table_alias_27.c
    Projection: #table_alias_25.a, #table_alias_25.b, #table_alias_25.c, alias=table_alias_25
      Projection: #test.a, #test.b, #test.c, alias=table_alias_25
        TableScan: test projection=None
    Projection: #table_alias_27.a, #table_alias_27.b, #table_alias_27.c, alias=table_alias_27
      Projection: #table_alias_26.a, #table_alias_26.b, #table_alias_26.c, alias=table_alias_27
        Filter: #table_alias_26.c < #table_alias_26.c
          Projection: #table_alias_26.a, #table_alias_26.b, #table_alias_26.c, alias=table_alias_26
            Projection: #test.a, #test.b, #test.c, alias=table_alias_26
              TableScan: test projection=None
```