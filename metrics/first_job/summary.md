# Riepilogo bench - first_job

Sorgente: `metrics/first_job/results.csv`
Politica: scartata la prima run (warm-up), media delle restanti.

## Tempi medi wall-clock (secondi)

| Framework | 50% | 100% | 200% | 400% |
|---|---|---|---|---|
| mapreduce | 43.0 | 48.0 | 58.5 | 76.0 |
| hiveql | 37.0 | 49.5 | 84.0 | 132.0 |
| hiveql_pipelined | 38.5 | 45.5 | 56.5 | 87.5 |
| sparkcore | 14.0 | 21.0 | 32.5 | 62.5 |
| sparksql | 13.5 | 16.0 | 26.0 | 45.5 |
