# Benchmark: runtime vs data size

Measured 2026-09-25T10:14:36+00:00 on macOS-27.0-arm64-arm-64bit-Mach-O, 8 CPUs, 17.2 GB RAM, Spark driver memory 4g. Versions: {'python': '3.13.5', 'duckdb': '1.5.5', 'pyarrow': '25.0.1', 'pyspark': '3.5.9'}.
Data: synthetic, one day per scale (1x = 100 devices x 3 metrics x 1/min). Spark runs in local mode on the same machine; this is not a cluster benchmark.

Transform time is the second of two runs for Spark (first run in brackets; only the first scale's first run includes JVM start-up).

| Scale | Engine | Raw rows | Raw MB | Transform s | (Spark 1st run s) | Readings/s |
|---|---|---|---|---|---|---|
| 1x | duckdb | 429,576 | 4.7 | 0.07 | – | 6,516,424 |
| 1x | spark | 429,576 | 4.7 | 1.94 | 7.93 | 221,780 |
| 10x | duckdb | 4,310,041 | 43.8 | 0.62 | – | 7,006,813 |
| 10x | spark | 4,310,041 | 43.8 | 6.22 | 7.56 | 693,429 |
| 100x | duckdb | 43,083,528 | 437.8 | 11.61 | – | 3,709,532 |
| 100x | spark | 43,083,528 | 437.8 | 56.79 | 63.9 | 758,628 |
