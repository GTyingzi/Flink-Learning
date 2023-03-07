package com.yingzi.chapter07;

import com.yingzi.chapter05.Source.ClickSource;
import com.yingzi.chapter05.Source.Event;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

/**
 * @author 影子
 * @create 2022-04-18-14:18
 **/
public class TopNExample_ProcessAllWindowFunctions {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //读取数据
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        //直接开窗，收集所有数据排序
        stream.map(data -> data.user)
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlHashMapCountAgg(), new UrlAllWindowResult())
                .print();

        env.execute();
    }

    //实现自定义的增量聚合函数
    private static class UrlHashMapCountAgg implements AggregateFunction<String, HashMap<String, Long>, ArrayList<Tuple2<String, Long>>> {
        @Override
        public HashMap<String, Long> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public HashMap<String, Long> add(String value, HashMap<String, Long> accumulator) {
            if (accumulator.containsKey(value)) {
                Long count = accumulator.get(value);
                accumulator.put(value, count + 1);
            } else {
                accumulator.put(value, 1L);
            }
            return accumulator;
        }

        @Override
        public ArrayList<Tuple2<String, Long>> getResult(HashMap<String, Long> accumulator) {
            ArrayList<Tuple2<String ,Long>> result = new ArrayList<>();
            for (String key : accumulator.keySet()){
                result.add(Tuple2.of(key,accumulator.get(key)));
            }
            result.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    return (int)(o2.f1 - o1.f1);
                }
            });
            return result;
        }

        @Override
        public HashMap<String, Long> merge(HashMap<String, Long> a, HashMap<String, Long> b) {
            return null;
        }
    }

    //实现自定义全窗口函数，包装信息输出结果
    private static class UrlAllWindowResult extends ProcessAllWindowFunction<ArrayList<Tuple2<String,Long>>,String, TimeWindow> {
        @Override
        public void process(Context context, Iterable<ArrayList<Tuple2<String, Long>>> elements, Collector<String> out) throws Exception {
            ArrayList<Tuple2<String, Long>> list = elements.iterator().next();

            StringBuilder result = new StringBuilder();
            result.append("--------------------------------\n");
            result.append("窗口结束时间：" + new Timestamp(context.window().getEnd()) + "\n");

            //取List前两个，包装信息输出
            for (int i = 0; i < 2; i++) {
                Tuple2<String, Long> currTuple = list.get(i);
                String info = "No. " + (i + 1) + " "
                        + "url: " + currTuple.f0 + " "
                        + "访问量" + currTuple.f1 + " \n";
                result.append(info);
            }
            result.append("--------------------------------\n");

            out.collect(result.toString());
        }
    }
}
