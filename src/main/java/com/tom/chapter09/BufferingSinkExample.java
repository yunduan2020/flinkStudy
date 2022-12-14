package com.tom.chapter09;

import com.tom.chapter05.ClickSource;
import com.tom.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class BufferingSinkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

//         env.enableCheckpointing(1000L);
//        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//        checkpointConfig.setCheckpointTimeout(60000L);
//        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        checkpointConfig.setMinPauseBetweenCheckpoints(500L);
//        checkpointConfig.setMaxConcurrentCheckpoints(1);
//        checkpointConfig.enableUnalignedCheckpoints();
//        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        checkpointConfig.setTolerableCheckpointFailureNumber(0);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long l) {
                                return element.timestamp;
                            }
                        }));

        stream.print("input");

        // ??????????????????
        stream.addSink(new BufferingSink(10));

        env.execute();
    }

    // ???????????????SinkFunction
    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction{

        // ?????????????????????????????????
        private final int threshold;
        private List<Event> bufferedElements;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        // ????????????????????????
        private ListState<Event> checkPointedState;

        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferedElements.add(value);    // ???????????????
            // ??????????????????????????????????????????
            if (bufferedElements.size() == threshold){
                // ?????????????????????????????????????????????
                for (Event element : bufferedElements){
                    System.out.println(element);
                }
                System.out.println("============????????????============");
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            // ????????????
            checkPointedState.clear();

            // ???????????????????????????????????????????????????????????????
            for(Event element : bufferedElements){
                checkPointedState.add(element);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // ??????????????????
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("buffered-elements", Event.class);
            checkPointedState = context.getOperatorStateStore().getListState(descriptor);

            // ?????????????????????????????????ListState????????????????????????????????????
            if(context.isRestored()){
                for (Event element : checkPointedState.get()){
                    bufferedElements.add(element);
                }
            }
        }
    }
}
