### Flink TPC-H Q3 Performance Benchmarking Project based on Cquirrel
This project benchmarks the performance of changed Apache Flink based on Cquirrel's BasedProcessFunction in executing the TPC-H Query 3 (Q3) using different configurations and optimizations. It leverages real-world data generated via DuckDB's TPC-H extension and evaluates Flink's execution efficiency under various threading and optimization strategies.

The goal of this project is to analyze and compare the performance of Flink when processing a complex analytical query (TPC-H Q3) under three distinct configurations:

1. flink_origin.java: Single-threaded execution
2. flink_4threads.java: 4-threaded execution
3. flink_optimize.java: 4-threaded execution with performance optimizations

Each configuration reads Parquet-formatted TPC-H data from disk using Flink’s batch streaming mode, then applies the stream processing, executes the SQL query, applies custom ProcessFunction logic for monitoring, rate limiting, throughput measurement.

* Note: tpch.ipynb is used to generate different scales of TPC-H data including scale_factor = 0.1，1，10，20 from duckdb.
* flink_origin_flink.java is the orginal version with flink's keyedprocessfunction, not Cquirrel's BasedProcessFunction.
* original-flink_benchmark-1.0-SNAPSHOT.jar is put in flink_benchmark\target file
