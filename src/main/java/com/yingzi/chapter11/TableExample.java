package com.yingzi.chapter11;

import com.yingzi.chapter05.Source.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author 影子
 * @create 2022-04-25-16:58
 **/
public class TableExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );

        //获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //将数据流转换成表
        Table eventTable = tableEnv.fromDataStream(eventStream);

        //用执行SQL的方式提取数据
        Table visitTable1 = tableEnv.sqlQuery("select url,user,`timestamp` from " + eventTable);

        //基于Table直接转换
        Table visitTable2 = eventTable.select($("user"), $("url"))
                .where($("user").isEqual("Alice"));

        //将表转换成数据流，打印输出
        tableEnv.toDataStream(visitTable1).print("table1");
        tableEnv.toDataStream(visitTable2).print("table2");

        env.execute();
    }
}
