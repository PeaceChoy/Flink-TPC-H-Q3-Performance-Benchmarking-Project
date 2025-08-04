package org.example;
// 使用precessionfunction
// 用了4线程

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.hkust.BasedProcessFunctions.BasedProcessFunction;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

public class flink_4threads {

    // 全局计数器
    private static final AtomicLong processedRecords = new AtomicLong(0);
    private static final AtomicLong resultCount = new AtomicLong(0);
    private static final AtomicLong lateRecords = new AtomicLong(0);

    // 性能统计时间戳
    private static long jobStartTime;
    private static long tableCreationStartTime;
    private static long tableCreationEndTime;
    private static long queryStartTime;
    private static long firstRecordTime = -1;
    private static long lastRecordTime = -1;
    private static long sinkStartTime;
    private static long sinkEndTime;

    // 侧输出标签，用于监控
    private static final OutputTag<String> STATS_OUTPUT = new OutputTag<String>("stats"){};
    private static final OutputTag<Row> LATE_DATA = new OutputTag<Row>("late-data"){};

    // 使用ProcessFunction进行流控制和监控
    public static class StreamControlProcessFunction extends ProcessFunction<Row, Row> {
        private final int recordsPerSecond;
        private transient long lastEmitTime;
        private transient int recordsInCurrentWindow;
        private transient long totalProcessed;

        // 性能统计
        private transient long lastStatsTime;
        private transient long lastStatsCount;

        public StreamControlProcessFunction(int recordsPerSecond) {
            this.recordsPerSecond = recordsPerSecond;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastEmitTime = System.currentTimeMillis();
            recordsInCurrentWindow = 0;
            totalProcessed = 0;
            lastStatsTime = System.currentTimeMillis();
            lastStatsCount = 0;
        }

        @Override
        public void processElement(Row value, Context ctx, Collector<Row> out) throws Exception {
            // 记录第一条记录的时间
            if (firstRecordTime == -1) {
                firstRecordTime = System.currentTimeMillis();
                System.out.println("[Timing] First record processed at: " +
                        new SimpleDateFormat("HH:mm:ss.SSS").format(new Date(firstRecordTime)));
            }

            totalProcessed++;
            processedRecords.incrementAndGet();

            long currentTime = System.currentTimeMillis();

            // 流控制逻辑
            if (currentTime - lastEmitTime > 1000) {
                // 新的时间窗口
                recordsInCurrentWindow = 0;
                lastEmitTime = currentTime;
            }

            if (recordsInCurrentWindow < recordsPerSecond) {
                // 正常输出
                out.collect(value);
                recordsInCurrentWindow++;

                // 每100条记录打印一次
                if (totalProcessed % 100 == 0) {
                    System.out.println(String.format("[Process] Record #%d: %s", totalProcessed, value));
                }
            } else {
                // 超过速率限制，发送到侧输出
                ctx.output(LATE_DATA, value);
                lateRecords.incrementAndGet();

                // 等待直到下一个时间窗口
                long sleepTime = 1000 - (currentTime - lastEmitTime);
                if (sleepTime > 0 && sleepTime < 1000) {
                    Thread.sleep(sleepTime);
                    lastEmitTime = System.currentTimeMillis();
                    recordsInCurrentWindow = 0;
                    out.collect(value);
                    recordsInCurrentWindow++;
                }
            }

            // 每5秒输出一次统计信息到侧输出
            if (currentTime - lastStatsTime > 5000) {
                long timeDiff = currentTime - lastStatsTime;
                long countDiff = totalProcessed - lastStatsCount;
                double throughput = countDiff / (timeDiff / 1000.0);

                String stats = String.format(
                        "Stats: Processed=%d, Throughput=%.2f rec/sec, Late=%d",
                        totalProcessed, throughput, lateRecords.get()
                );

                ctx.output(STATS_OUTPUT, stats);

                lastStatsTime = currentTime;
                lastStatsCount = totalProcessed;
            }
        }

        @Override
        public void close() throws Exception {
            // 记录处理结束时间
            if (totalProcessed > 0) {
                lastRecordTime = System.currentTimeMillis();
            }

            System.out.println("[StreamControl] Closing, total processed: " + totalProcessed);

            // 如果没有处理任何记录，设置默认值
            if (firstRecordTime == -1) {
                firstRecordTime = System.currentTimeMillis();
            }

            super.close();
        }
    }

//    // 使用KeyedProcessFunction进行监控和统计
//    public static class MonitoringProcessFunction extends KeyedProcessFunction<Long, Row, Row> {
//        // 状态：存储每个key的记录数
//        private ValueState<Long> recordCountState;
//        private ValueState<Long> firstSeenTimeState;
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            super.open(parameters);
//
//            // 初始化状态
//            recordCountState = getRuntimeContext().getState(
//                    new ValueStateDescriptor<>("recordCount", Long.class, 0L));
//            firstSeenTimeState = getRuntimeContext().getState(
//                    new ValueStateDescriptor<>("firstSeenTime", Long.class));
//        }
//
//        @Override
//        public void processElement(Row value, Context ctx, Collector<Row> out) throws Exception {
//            // 获取当前状态
//            Long count = recordCountState.value();
//            Long firstTime = firstSeenTimeState.value();
//
//            // 第一次看到这个key
//            if (firstTime == null) {
//                firstTime = System.currentTimeMillis();
//                firstSeenTimeState.update(firstTime);
//            }
//
//            // 更新计数
//            recordCountState.update(count + 1);
//
//            // 输出原始记录
//            out.collect(value);
//
//            // 每个key的第1条和第10条记录打印详细信息
//            if (count == 0 || count == 9) {
//                Long orderKey = (Long) value.getField(0);
//                java.math.BigDecimal revenue = (java.math.BigDecimal) value.getField(1);
//                System.out.println(String.format(
//                        "[Key %d] Record #%d: revenue=%.2f, processing_time=%dms",
//                        orderKey, count + 1, revenue.doubleValue(),
//                        System.currentTimeMillis() - firstTime
//                ));
//            }
//        }
//
//        @Override
//        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Row> out) throws Exception {
//            // 定时器触发时可以清理状态或输出统计信息
//            Long count = recordCountState.value();
//            if (count != null && count > 0) {
//                System.out.println(String.format(
//                        "[Key %d] Total records processed: %d",
//                        ctx.getCurrentKey(), count
//                ));
//            }
//            // 清理状态
//            recordCountState.clear();
//            firstSeenTimeState.clear();
//        }
//    }
    //changed
    // 使用BasedProcessFunction替代KeyedProcessFunction进行监控
    @SuppressWarnings({"unchecked", "rawtypes"})  // 抑制Scala-Java互操作警告
    public static class MonitoringBasedProcessFunction extends BasedProcessFunction<Long, Row, Row> {
        // 状态：存储每个key的记录数
        private ValueState<Long> recordCountState;
        private ValueState<Long> firstSeenTimeState;

        // 保存窗口参数
        private final long windowLengthParam;
        private final long slidingSizeParam;
        private final String functionName;

        // 用于访问父类的统计变量
        private long localStoreAccur = 0L;

        // 构造函数，设置窗口长度为10秒，滑动间隔为1秒
        public MonitoringBasedProcessFunction() {
            super(10000L, 1000L, "MonitoringProcess", false);
            this.windowLengthParam = 10000L;
            this.slidingSizeParam = 1000L;
            this.functionName = "MonitoringProcess";
        }

        @Override
        public void initstate() {
            // 初始化自定义状态
            // 使用新的 ValueStateDescriptor 构造函数（不带默认值）
            recordCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("recordCount", Long.class));
            firstSeenTimeState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("firstSeenTime", Long.class));
        }

        @Override
        public void process(Row value_raw, KeyedProcessFunction<Long, Row, Row>.Context ctx, Collector<Row> out) {
            try {
                long startTime = System.currentTimeMillis();

                // 获取当前状态，如果为null则使用默认值
                Long count = recordCountState.value();
                if (count == null) {
                    count = 0L;
                }

                Long firstTime = firstSeenTimeState.value();

                // 第一次看到这个key
                if (firstTime == null) {
                    firstTime = System.currentTimeMillis();
                    firstSeenTimeState.update(firstTime);
                }

                // 更新计数
                recordCountState.update(count + 1);

                // 输出原始记录
                out.collect(value_raw);

                // 每个key的第1条和第10条记录打印详细信息
                if (count == 0 || count == 9) {
                    Long orderKey = (Long) value_raw.getField(0);
                    java.math.BigDecimal revenue = (java.math.BigDecimal) value_raw.getField(1);
                    System.out.println(String.format(
                            "[Key %d] Record #%d: revenue=%.2f, processing_time=%dms",
                            orderKey, count + 1, revenue.doubleValue(),
                            System.currentTimeMillis() - firstTime
                    ));
                }

                // 更新本地统计时间
                localStoreAccur += System.currentTimeMillis() - startTime;

                // 定期打印统计信息
                if (count % 100 == 0) {
                    System.out.println(String.format(
                            "[BasedProcessFunction Stats] Key=%d, Records=%d, LocalStoreTime=%dms",
                            ctx.getCurrentKey(), count, localStoreAccur
                    ));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void enumeration(Collector<Row> out) {
            // 这个方法可以用于定期枚举和输出聚合结果
            // 在这个例子中我们不需要额外的枚举逻辑
            try {
                Long count = recordCountState.value();
                if (count != null && count > 0) {
                    System.out.println(String.format(
                            "[Enumeration] Total records for current key: %d",
                            count
                    ));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void expire(KeyedProcessFunction<Long, Row, Row>.Context ctx) {
            // 处理过期数据的逻辑
            try {
                Long currentTime = System.currentTimeMillis();
                Long firstTime = firstSeenTimeState.value();

                if (firstTime != null && currentTime - firstTime > windowLengthParam) {
                    // 清理过期状态
                    System.out.println(String.format(
                            "[Key %d] Expiring data older than %d ms",
                            ctx.getCurrentKey(), windowLengthParam
                    ));

                    // 可以在这里清理状态
                    // recordCountState.clear();
                    // firstSeenTimeState.clear();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void storeStream(Row value, KeyedProcessFunction<Long, Row, Row>.Context ctx) {
            // 存储流数据的逻辑
            // 这个方法在基类中被标记为deprecated，但需要实现
            // 可以在这里实现缓存逻辑
        }

        @Override
        public boolean testExists(Row value, KeyedProcessFunction<Long, Row, Row>.Context ctx) {
            // 测试元素是否已存在或是否合法
            // 返回true表示需要处理这个元素
            return true;
        }

        @Override
        public void processBuffer(Row value, KeyedProcessFunction<Long, Row, Row>.Context ctx, Collector<Row> out) {
            // 处理缓冲区中的元素，用于处理乱序数据
            // 在这个例子中使用默认的空实现
        }

        @Override
        public void close() {
            // 注意：不要声明 throws Exception，因为父类没有声明
            // 在关闭时输出最终统计
            System.out.println(String.format(
                    "[%s] Closing - Local processing time: %d ms",
                    functionName, localStoreAccur
            ));

            // 调用父类的 close 方法
            super.close();
        }
    }

    // 监控Sink
    public static class MonitoringSink extends RichSinkFunction<Row> {
        private long startTime;
        private final AtomicLong localCount = new AtomicLong(0);
        private long lastReportTime;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            startTime = System.currentTimeMillis();
            sinkStartTime = startTime;  // 记录全局sink开始时间
            lastReportTime = startTime;
            System.out.println("[MonitoringSink] Initialized at: " +
                    new SimpleDateFormat("HH:mm:ss.SSS").format(new Date(startTime)));
        }

        @Override
        public void invoke(Row value, Context context) throws Exception {
            long count = localCount.incrementAndGet();
            resultCount.incrementAndGet();

            // 更新最后记录时间
            lastRecordTime = System.currentTimeMillis();

            // 打印前10条结果
            if (count <= 10) {
                System.out.println(String.format("[Result #%d] %s", count, value.toString()));
            }
        }

        @Override
        public void close() throws Exception {
            long endTime = System.currentTimeMillis();
            sinkEndTime = endTime;  // 记录全局sink结束时间
            long duration = endTime - startTime;

            System.out.println("\n========== Sink Metrics ==========");
            System.out.println("Total Records: " + localCount.get());
            System.out.println("Sink Duration: " + duration + " ms");
            System.out.println("Sink Throughput: " + String.format("%.2f", localCount.get() / (duration / 1000.0)) + " rec/sec");
            System.out.println("==================================\n");

            super.close();
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("TPC-H Q3 - ProcessFunction Streaming Implementation");
        System.out.println("===================================================\n");

        jobStartTime = System.currentTimeMillis();
        System.out.println("Job started at: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(jobStartTime)));

        // 配置
        Configuration config = new Configuration();
        config.setString("taskmanager.memory.process.size", "4g");
        config.setString("taskmanager.memory.managed.size", "2g");
        config.setInteger("taskmanager.numberOfTaskSlots", 4);
        config.setBoolean("local.start-webserver", true);
        config.setInteger("rest.port", 8081);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

        // 使用批处理模式读取文件，然后用ProcessFunction处理
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(4);
        env.getConfig().enableObjectReuse();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取文件路径
        String projectDir = System.getProperty("user.dir");
        String lineitemPath = projectDir + "/tpch_data/lineitem.parquet";
        String customerPath = projectDir + "/tpch_data/customer.parquet";
        String ordersPath = projectDir + "/tpch_data/orders.parquet";

        System.out.println("Data files:");
        System.out.println("  - " + lineitemPath);
        System.out.println("  - " + customerPath);
        System.out.println("  - " + ordersPath);
        System.out.println();

        // 记录表创建开始时间
        tableCreationStartTime = System.currentTimeMillis();
        System.out.println("Creating tables at: " + new SimpleDateFormat("HH:mm:ss.SSS").format(new Date(tableCreationStartTime)));

        // 创建表（批处理读取）
        tableEnv.executeSql(
                "CREATE TABLE lineitem (" +
                        "  l_orderkey BIGINT," +
                        "  l_partkey BIGINT," +
                        "  l_suppkey BIGINT," +
                        "  l_linenumber INT," +
                        "  l_quantity DECIMAL(15, 2)," +
                        "  l_extendedprice DECIMAL(15, 2)," +
                        "  l_discount DECIMAL(15, 2)," +
                        "  l_tax DECIMAL(15, 2)," +
                        "  l_returnflag STRING," +
                        "  l_linestatus STRING," +
                        "  l_shipdate DATE," +
                        "  l_commitdate DATE," +
                        "  l_receiptdate DATE," +
                        "  l_shipinstruct STRING," +
                        "  l_shipmode STRING," +
                        "  l_comment STRING" +
                        ") WITH (" +
                        "  'connector' = 'filesystem'," +
                        "  'path' = '" + lineitemPath + "'," +
                        "  'format' = 'parquet'" +
                        ")"
        );

        tableEnv.executeSql(
                "CREATE TABLE customer (" +
                        "  c_custkey BIGINT," +
                        "  c_name STRING," +
                        "  c_address STRING," +
                        "  c_nationkey INT," +
                        "  c_phone STRING," +
                        "  c_acctbal DECIMAL(15, 2)," +
                        "  c_mktsegment STRING," +
                        "  c_comment STRING" +
                        ") WITH (" +
                        "  'connector' = 'filesystem'," +
                        "  'path' = '" + customerPath + "'," +
                        "  'format' = 'parquet'" +
                        ")"
        );

        tableEnv.executeSql(
                "CREATE TABLE orders (" +
                        "  o_orderkey BIGINT," +
                        "  o_custkey BIGINT," +
                        "  o_orderstatus STRING," +
                        "  o_totalprice DECIMAL(15, 2)," +
                        "  o_orderdate DATE," +
                        "  o_orderpriority STRING," +
                        "  o_clerk STRING," +
                        "  o_shippriority INT," +
                        "  o_comment STRING" +
                        ") WITH (" +
                        "  'connector' = 'filesystem'," +
                        "  'path' = '" + ordersPath + "'," +
                        "  'format' = 'parquet'" +
                        ")"
        );

        // 记录表创建结束时间
        tableCreationEndTime = System.currentTimeMillis();
        System.out.println("Tables created at: " + new SimpleDateFormat("HH:mm:ss.SSS").format(new Date(tableCreationEndTime)));
        System.out.println("Table creation took: " + (tableCreationEndTime - tableCreationStartTime) + " ms\n");

        // TPC-H Q3查询
        String q3Query =
                "SELECT " +
                        "  l_orderkey, " +
                        "  SUM(l_extendedprice * (1 - l_discount)) AS revenue, " +
                        "  o_orderdate, " +
                        "  o_shippriority " +
                        "FROM lineitem " +
                        "JOIN orders ON l_orderkey = o_orderkey " +
                        "JOIN customer ON o_custkey = c_custkey " +
                        "WHERE " +
                        "  c_mktsegment = 'BUILDING' AND " +
                        "  o_orderdate < DATE '1995-03-15' AND " +
                        "  l_shipdate > DATE '1995-03-15' " +
                        "GROUP BY l_orderkey, o_orderdate, o_shippriority " +
                        "ORDER BY revenue DESC, o_orderdate";

        System.out.println("Executing TPC-H Q3 with ProcessFunction...\n");

        // 记录查询开始时间
        queryStartTime = System.currentTimeMillis();
        System.out.println("Query execution started at: " + new SimpleDateFormat("HH:mm:ss.SSS").format(new Date(queryStartTime)));

        Table result = tableEnv.sqlQuery(q3Query);
        DataStream<Row> resultStream = tableEnv.toDataStream(result);

        // 定义Row类型信息 - 注意类型匹配
        TypeInformation<Row> rowTypeInfo = new RowTypeInfo(
                Types.LONG,        // l_orderkey
                Types.BIG_DEC,     // revenue (BigDecimal)
                Types.LOCAL_DATE,  // o_orderdate (LocalDate)
                Types.INT          // o_shippriority
        );

        // 应用ProcessFunction进行流控制
        int rateLimit = 5000; // 每秒5000条
        SingleOutputStreamOperator<Row> processedStream = resultStream
                .process(new StreamControlProcessFunction(rateLimit))
                .returns(rowTypeInfo)
                .name("Stream Control (" + rateLimit + " rec/sec)");

        // 获取侧输出流
        DataStream<String> statsStream = processedStream.getSideOutput(STATS_OUTPUT);
        DataStream<Row> lateDataStream = processedStream.getSideOutput(LATE_DATA);

        // 打印统计信息
        statsStream.print("STATS").setParallelism(1);

        // 处理延迟数据
        lateDataStream
                .map(row -> "Late data: " + row.toString())
                .print("LATE").setParallelism(1);

//        // 应用KeyedProcessFunction进行监控
//        DataStream<Row> monitoredStream = processedStream
//                .keyBy(row -> (Long) row.getField(0)) // 按orderkey分组
//                .process(new MonitoringProcessFunction())
//                .returns(rowTypeInfo)
//                .name("Monitoring Process Function");

        // 应用BasedProcessFunction进行监控
        //changed
        DataStream<Row> monitoredStream = processedStream
                .keyBy(row -> (Long) row.getField(0)) // 按orderkey分组
                .process(new MonitoringBasedProcessFunction())
                .returns(rowTypeInfo)
                .name("Monitoring Based Process Function");

        // 添加最终的Sink
        monitoredStream
                .addSink(new MonitoringSink())
                .setParallelism(1)
                .name("Final Output Sink");

        // 全局统计线程
        Thread globalStatsThread = new Thread(() -> {
            long startTime = System.currentTimeMillis();
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Thread.sleep(10000);
                    long elapsed = System.currentTimeMillis() - startTime;
                    System.out.println("\n========== Global Statistics ==========");
                    System.out.println("Elapsed: " + (elapsed / 1000) + " seconds");
                    System.out.println("Processed: " + processedRecords.get());
                    System.out.println("Results: " + resultCount.get());
                    System.out.println("Late Records: " + lateRecords.get());
                    System.out.println("Input Rate: " + String.format("%.2f",
                            processedRecords.get() / (elapsed / 1000.0)) + " rec/sec");
                    System.out.println("Output Rate: " + String.format("%.2f",
                            resultCount.get() / (elapsed / 1000.0)) + " rec/sec");
                    System.out.println("=====================================\n");
                }
            } catch (InterruptedException e) {
                // 线程被中断，正常退出
            }
        });
        globalStatsThread.setDaemon(true);
        globalStatsThread.start();

        System.out.println("Web UI: http://localhost:8081");
        System.out.println("Features:");
        System.out.println("- ProcessFunction for flow control and rate limiting");
        System.out.println("- KeyedProcessFunction for per-key monitoring");
        System.out.println("- Side outputs for statistics and late data handling");
        System.out.println("- State management for tracking metrics");
        System.out.println("- Batch reading with streaming-like processing");
        System.out.println("\nStarting execution...\n");

        // 执行
        long executeStartTime = System.currentTimeMillis();
        env.execute("TPC-H Q3 - ProcessFunction Implementation");
        long jobEndTime = System.currentTimeMillis();

        // 停止统计线程
        globalStatsThread.interrupt();

        // 等待一下，确保所有输出完成
        Thread.sleep(100);

        // 输出性能汇总报告
        System.out.println("\n\n========================================");
        System.out.println("    PERFORMANCE SUMMARY REPORT");
        System.out.println("========================================");

        // 计算各阶段时间
        long totalJobTime = jobEndTime - jobStartTime;
        long tableSetupTime = tableCreationEndTime - tableCreationStartTime;

        // 确保firstRecordTime有值
        if (firstRecordTime == -1) {
            firstRecordTime = sinkStartTime;
        }

        // 确保lastRecordTime有值
        if (lastRecordTime == -1) {
            lastRecordTime = sinkEndTime;
        }

        long queryPlanningTime = firstRecordTime - queryStartTime;  // 从查询开始到第一条记录
        long dataProcessingTime = lastRecordTime - firstRecordTime;  // 从第一条到最后一条记录
        long sinkProcessingTime = sinkEndTime - sinkStartTime;
        long flinkOverheadTime = totalJobTime - tableSetupTime - queryPlanningTime - dataProcessingTime;

        System.out.println("Total Job Execution Time: " + totalJobTime + " ms");
        System.out.println("\nDetailed Breakdown:");
        System.out.println("├─ Table Creation Time:    " + String.format("%6d ms (%5.1f%%)",
                tableSetupTime, 100.0 * tableSetupTime / totalJobTime));
        System.out.println("├─ Query Planning Time:    " + String.format("%6d ms (%5.1f%%)",
                queryPlanningTime, 100.0 * queryPlanningTime / totalJobTime));
        System.out.println("├─ Data Processing Time:   " + String.format("%6d ms (%5.1f%%)",
                dataProcessingTime, 100.0 * dataProcessingTime / totalJobTime));
        System.out.println("│  └─ Sink Time:          " + String.format("%6d ms (%5.1f%% of processing)",
                sinkProcessingTime, dataProcessingTime > 0 ? 100.0 * sinkProcessingTime / dataProcessingTime : 0));
        System.out.println("└─ Framework Overhead:     " + String.format("%6d ms (%5.1f%%)",
                flinkOverheadTime, 100.0 * flinkOverheadTime / totalJobTime));

        System.out.println("\nThroughput Metrics:");
        if (totalJobTime > 0) {
            System.out.println("├─ Overall Throughput:     " + String.format("%.2f records/sec",
                    resultCount.get() / (totalJobTime / 1000.0)));
        }
        if (dataProcessingTime > 0) {
            System.out.println("├─ Processing Throughput:  " + String.format("%.2f records/sec",
                    resultCount.get() / (dataProcessingTime / 1000.0)));
        }
        if (sinkProcessingTime > 0) {
            System.out.println("└─ Sink Throughput:        " + String.format("%.2f records/sec",
                    resultCount.get() / (sinkProcessingTime / 1000.0)));
        }

        System.out.println("\nData Statistics:");
        System.out.println("├─ Total Records Processed: " + processedRecords.get());
        System.out.println("├─ Total Records Output:    " + resultCount.get());
        System.out.println("└─ Late Records:           " + lateRecords.get());

        System.out.println("\nNote: Query planning includes file I/O, SQL parsing, optimization, and job initialization.");
        System.out.println("      Data processing includes actual computation, joins, aggregations, and sorting.");
        System.out.println("========================================\n");
    }
}