package com.yingzi.chapter05.Transform;

import com.yingzi.chapter05.Source.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @author 影子
 * @create 2022-04-13-17:09
 **/
public class TransformFlatMapTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

        //1.传入一个实现了FlatMapFunction的类对象
        SingleOutputStreamOperator result1 = stream.flatMap(new MyFlatMap());

        //2.传入Lambda表达式
        SingleOutputStreamOperator<String> result2 = stream.flatMap((Event event, Collector<String> out) -> {
            if (event.user.equals("Mary"))
                out.collect(event.url);
            else if (event.user.equals("Bob")) {
                out.collect(event.user);
                out.collect(event.url);
                out.collect(event.timestamp.toString());
            }
        }).returns(new TypeHint<String>() {});

        result1.print("1");
        result2.print("2");
//        result3.print();


        env.execute();
    }

    //实现一个自定义的FlatMapFunction
    private static class MyFlatMap implements FlatMapFunction<Event, String> {

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            collector.collect(event.user);
            collector.collect(event.url);
            collector.collect(event.timestamp.toString());
        }
    }
}
