package com.tom.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class SourceTest {
    public static void main(String[] args) throws Exception{
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从文件读取数据
        DataStreamSource<String> stream1 = env.readTextFile("input/click.txt");

        // 2. 从集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(1);
        nums.add(3);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cart", 2000L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);

        // 3. 从元素读取数据
        DataStreamSource<Event> stream3 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 4. 从Socket文本流中读取
        DataStreamSource<String> stream4 = env.socketTextStream("hadoop102", 7777);

        // 5. 从kafka中读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));


//        stream1.print("1");
//        numStream.print("nums");
//        stream2.print("2");
//        stream3.print("3");
//        stream4.print("4");
        kafkaStream.print();

        env.execute();

    }
}
