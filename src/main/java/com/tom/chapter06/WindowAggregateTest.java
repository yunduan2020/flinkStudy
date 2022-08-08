package com.tom.chapter06;

import com.tom.chapter05.ClickSource;
import com.tom.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;

public class WindowAggregateTest {
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

        stream.keyBy(data -> data.user)
                        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                                .aggregate(new AggregateFunction<Event, Tuple2<Long, Integer>, String>() {
                                    // 累加器初始值
                                    @Override
                                    public Tuple2<Long, Integer> createAccumulator() {
                                        return Tuple2.of(0L, 0);
                                    }

                                    @Override
                                    public Tuple2<Long, Integer> add(Event value, Tuple2<Long, Integer> accumulator) {
                                        return Tuple2.of(accumulator.f0 + value.timestamp, accumulator.f1 + 1);
                                    }

                                    @Override
                                    public String getResult(Tuple2<Long, Integer> accumulator) {
                                        Timestamp timestamp = new Timestamp(accumulator.f0 / accumulator.f1);
                                        return timestamp.toString();
                                    }

                                    @Override
                                    public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
                                        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                                    }
                                })
                                        .print();

        env.execute();
    }
}
