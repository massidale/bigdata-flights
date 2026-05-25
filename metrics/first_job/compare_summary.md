# first_job - confronto Locale vs EMR

## Tempi medi wall-clock (secondi)

**Locale** (Mac single-node, Spark local[*])

| Framework | 50% | 100% | 200% | 400% |
|---|---|---|---|---|
| mapreduce | 43.0 | 48.0 | 58.5 | 76.0 |
| hiveql_pipelined | 38.5 | 45.5 | 56.5 | 87.5 |
| sparkcore | 14.0 | 21.0 | 32.5 | 62.5 |
| sparksql | 13.5 | 16.0 | 26.0 | 45.5 |

**EMR** (1 master + 4 core m5.xlarge, Spark on YARN, Hive on Tez)

| Framework | 50% | 100% | 200% | 400% |
|---|---|---|---|---|
| mapreduce | 110.4 | 114.2 | 120.1 | 150.2 |
| hiveql_pipelined | 52.1 | 60.1 | 68.1 | 78.1 |
| sparkcore | 42.1 | 44.1 | 52.1 | 70.1 |
| sparksql | 38.1 | 38.1 | 48.1 | 48.1 |