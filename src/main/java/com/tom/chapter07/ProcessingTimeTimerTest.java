package com.tom.chapter07;

import com.tom.chapter05.ClickSource;
import com.tom.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class ProcessingTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());

        stream.keyBy(data -> data.user)
                        .process(new KeyedProcessFunction<String, Event, String>() {
                            @Override
                            public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                                Long currTs = ctx.timerService().currentProcessingTime();
                                out.collect(ctx.getCurrentKey() + " 数据到达，到达时间： " + new Timestamp(currTs));

                                // 注册一个10秒后的定时器
                                ctx.timerService().registerProcessingTimeTimer(currTs + 10 * 1000L);
                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                out.collect(ctx.getCurrentKey() + " 定时器触发，触发时间： " + new Timestamp(timestamp));
                            }
                        }).print();
        env.execute();
    }
}
