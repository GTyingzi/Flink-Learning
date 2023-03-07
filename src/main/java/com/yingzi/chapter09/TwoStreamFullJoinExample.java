package com.yingzi.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author 影子
 * @create 2022-04-22-10:06
 **/
public class TwoStreamFullJoinExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env.fromElements(
                Tuple3.of("a", "stream-1", 1000L),
                Tuple3.of("b", "stream-1", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                }));

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env.fromElements(
                Tuple3.of("a", "stream-2", 3000L),
                Tuple3.of("b", "stream-2", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                }));

        stream1.keyBy(data -> data.f0)
                .connect(stream2.keyBy(data -> data.f0))
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    private ListState<Tuple3<String ,String ,Long>> stream1ListStae;
                    private ListState<Tuple3<String ,String ,Long>> stream2ListStae;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        stream1ListStae = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String ,String ,Long>>("stream1-list", Types.TUPLE(Types.STRING,Types.STRING)));
                        stream2ListStae = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String ,String ,Long>>("stream2-list", Types.TUPLE(Types.STRING,Types.STRING)));
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        stream1ListStae.add(value);
                        for (Tuple3<String, String, Long> stream2 : stream2ListStae.get()) {
                            out.collect(value + " => " + stream2);
                        }
                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        for (Tuple3<String, String, Long> stream1 : stream1ListStae.get()) {
                            out.collect(stream1 + " => " + value);
                        }
                    }
                }).print();

        env.execute();
    }
}
