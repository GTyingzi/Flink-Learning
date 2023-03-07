package com.yingzi.chapter07;

import com.yingzi.chapter05.Source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author 影子
 * @create 2022-04-18-13:38
 **/
public class EventTimeTimerTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        //事件时间
        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        Long currTs = ctx.timestamp();
                        out.collect(ctx.getCurrentKey() + " 数据到达，到达时间：" + new Timestamp(currTs) + " watermark: " + ctx.timerService().currentWatermark());

                        //注册一个10秒后的定时器
                        ctx.timerService().registerEventTimeTimer(currTs + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(" 定时器触发，触发时间：" + new Timestamp(timestamp) + " watermark: " + ctx.timerService().currentWatermark());
                    }
                }).print();

        env.execute();
    }


    //自定义测试数据源
    public static class CustomSource implements SourceFunction<Event> {

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            //直接发出测试数据
            ctx.collect(new Event("Marry", "./home", 1000L));
            Thread.sleep(5000L);

            ctx.collect(new Event("Alice", "./home", 11000L));
            Thread.sleep(5000L);

            ctx.collect(new Event("Bob", "./home", 11001L));
            Thread.sleep(5000L);
        }

        @Override
        public void cancel() {

        }
    }
}
