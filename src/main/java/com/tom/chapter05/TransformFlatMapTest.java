package com.tom.chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

        // 实现一个自定义的FlatMapFunction
        SingleOutputStreamOperator<String> result1 = stream.flatMap(new MyFlatMap());

        // 传入一个lambda表达式
        SingleOutputStreamOperator<String> result2 = stream.flatMap((Event value, Collector<String> out) -> {
            if (value.user.equals("Mary")) {
                out.collect(value.url);
            } else if (value.user.equals("Bob")) {
                out.collect(value.user);
                out.collect(value.url);
                out.collect(value.timestamp.toString());
            }
        }).returns(new TypeHint<String>() {});

        result1.print("1");
        result2.print("2");

        env.execute();

    }

    public static class MyFlatMap implements FlatMapFunction<Event, String>{
        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            out.collect(value.user);
            out.collect(value.url);
            out.collect(value.timestamp.toString());
        }
    }
}
