package com.tom.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class  TransformFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

        SingleOutputStreamOperator<Event> result1 = stream.filter(new MyFilter());

        SingleOutputStreamOperator<Event> result2 = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.user.equals("Bob");
            }
        });

        stream.filter(data -> data.user.equals("Alice")).print("lambda:Alice click");

        result1.print("1");
        result2.print("2");

        env.execute();
    }

    public static class MyFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event event) throws Exception {
            return event.user.equals("Mary");
        }
    }
}
