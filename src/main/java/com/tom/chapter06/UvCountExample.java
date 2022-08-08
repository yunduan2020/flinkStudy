package com.tom.chapter06;

import com.tom.chapter05.ClickSource;
import com.tom.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class UvCountExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                // 乱序流的Watermark生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long l) {
                                return element.timestamp;
                            }
                        }));

        stream.print("input");

        // 使用AggregateFunction和ProcessWindowFunction计算UV
        stream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UvAgg(), new UvCountResult())
                .print();

        env.execute();
    }

    // 自定义实现AggregateFunction，增量聚合计算uv值
    public static class UvAgg implements AggregateFunction<Event, HashSet<String>, Long>{

        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(Event value, HashSet<String> accumulator) {
            accumulator.add(value.user);
            return accumulator;
        }

        @Override
        public Long getResult(HashSet<String> accumulator) {
            return (long)accumulator.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> strings, HashSet<String> acc1) {
            return null;
        }
    }

    // 自定义实现ProcessWindowFunction，包装窗口信息输出
    public static class UvCountResult extends ProcessWindowFunction<Long, String, Boolean, TimeWindow>{

        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Long, String, Boolean, TimeWindow>.Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            Long uv = elements.iterator().next();
            out.collect("窗口" + new Timestamp(start) + "~" + new Timestamp(end) + "UV值为：" + uv);
        }
    }
}
