package com.tom.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.omg.CORBA.UserException;

public class TransformMapTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

        // 进行转换计算，提取user字段
        // 1. 使用自定义类，实现MapFunction接口
        SingleOutputStreamOperator<String> result1 = stream.map(new MyMapper());

        // 2. 使用匿名类实现MapFunction接口
        SingleOutputStreamOperator<String> result2 = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event e) throws Exception {
                return e.user;
            }
        });

        // 3. 传入lambda表达式
        SingleOutputStreamOperator<String> result3 = stream.map(data -> data.user);

        result1.print("1");
        result2.print("2");
        result3.print("3");

        env.execute();
    }

    public static class MyMapper implements MapFunction<Event, String>{
        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}
