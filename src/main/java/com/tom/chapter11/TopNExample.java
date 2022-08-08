package com.tom.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TopNExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 在创建表的DDL中直接定义时间属性
        String creatDDL = "CREATE TABLE clickTable (" +
                "`user` STRING, " +
                "url STRING, " +
                "ts BIGINT, " +
                "et AS TO_TIMESTAMP( FROM_UNIXTIME(ts / 1000) ), " +
                "WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'input/click.txt'," +
                " 'format' = 'csv'" +
                ")";
        tableEnv.executeSql(creatDDL);

        // 普通Top N， 选取当前所有用户中浏览量最大的2个
        Table topNResultTable = tableEnv.sqlQuery("SELECT user, cnt, row_num " +
                " FROM (" +
                " SELECT * , ROW_NUMBER() OVER(" +
                " ORDER BY cnt DESC " +
                " ) AS row_num " +
                " FROM (SELECT user, COUNT(url) as cnt FROM clickTable GROUP BY user ) " +
                ") WHERE row_num <= 2");

        // tableEnv.toChangelogStream(topNResultTable).print("top 2: ");

        // 窗口TOP N，统计一段时间内的（前2名）活跃用户
        String subQuery = "SELECT user, COUNT(url) AS cnt, window_start, window_end " +
                " FROM TABLE (" +
                " TUMBLE(TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND)" +
                ")" +
                "GROUP BY user, window_start, window_end";

        Table windowTopNResultTable = tableEnv.sqlQuery("SELECT user, cnt, row_num, window_end " +
                " FROM (" +
                " SELECT * , ROW_NUMBER() OVER(" +
                "   PARTITION BY window_start, window_end " +
                "   ORDER BY cnt DESC " +
                " ) AS row_num " +
                " FROM (" + subQuery + " ) " +
                ") WHERE row_num <= 2");
        tableEnv.toDataStream(windowTopNResultTable).print("window top n: ");

        env.execute();
    }
}
