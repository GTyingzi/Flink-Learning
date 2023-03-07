package com.yingzi.chapter05.Transform;

import com.yingzi.chapter05.Source.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 影子
 * @create 2022-04-13-16:06
 **/
public class TransformMapTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        //进行转换计算，提取user字段
        //使用自定类，实现MapFunction接口
        SingleOutputStreamOperator<String> result1 = stream.map(new MyMappper());

        //2.使用匿名类实现MapFunction接口
        SingleOutputStreamOperator<String> result2 = stream.map(new MapFunction<Event, String>() {

            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }
        });

        //3.传入Lambda表达式
        SingleOutputStreamOperator<String> result3 = stream.map(data -> data.user);


        result1.print();
        result2.print();
        result3.print();

        env.execute();
    }

    //自定义MapFunction
    public static class MyMappper implements MapFunction<Event,String>{

        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}
