package com.tom.chapter11;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class CommonApiTest {
    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 定义环境配置来创建表执行环境

        // 基于blink版本planner进行流处理
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

//        // 1.1 基于老版本planner进行流处理
//        EnvironmentSettings settings1 = EnvironmentSettings.newInstance()
//                .inStreamingMode()
//                .useOldPlanner()
//                .build();
//
//        TableEnvironment tableEnv1 = TableEnvironment.create(settings1);
//
//        // 1.2 基于老版本planner进行批处理
//        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(batchEnv);
//
//        // 1.3 基于blink版本planner进行批处理
//        EnvironmentSettings settings3 = EnvironmentSettings.newInstance()
//                .inBatchMode()
//                .useBlinkPlanner()
//                .build();
//
//        TableEnvironment tableEnv3 = TableEnvironment.create(settings3);

        // 2. 创建表
        String creatDDL = "CREATE TABLE clickTable (" +
                "user_name STRING, " +
                "url STRING, " +
                "ts BIGINT " +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'input/click.txt'," +
                " 'format' = 'csv'" +
                ")";

        tableEnv.executeSql(creatDDL);

        // 调用Table API进行表的查询转换
        Table clickTable  = tableEnv.from("clickTable");
        Table resultTable = clickTable.where($("user_name").isEqual("Bob"))
                .select($("user_name"),$("url"));

        tableEnv.createTemporaryView("result2", resultTable);

        // 执行SQL进行表的查询转换
        Table resultTable2 = tableEnv.sqlQuery("select url, user_name from result2");

        // 执行聚合计算的查询转换
        Table aggResult = tableEnv.sqlQuery("select user_name, COUNT(url) as cnt from clickTable group by user_name");

        // 创建一张用于输出的表
        String creatOutDDL = "CREATE TABLE outTable (" +
                "user_name STRING, " +
                "url STRING " +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'output'," +
                " 'format' = 'csv'" +
                ")";

        tableEnv.executeSql(creatOutDDL);

        // 创建一张用于控制台打印输出的表
        String creatPrintOutDDL = "CREATE TABLE printOutTable (" +
                "user_name STRING, " +
                "cnt BIGINT " +
                ") WITH (" +
                " 'connector' = 'print'" +
                ")";

        tableEnv.executeSql(creatPrintOutDDL);

        // 输出表
//        resultTable.executeInsert("outTable");
//        resultTable2.executeInsert("printOutTable");
        aggResult.executeInsert("printOutTable");

    }
}
