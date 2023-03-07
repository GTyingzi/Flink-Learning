package com.yingzi.chapter11;

import com.yingzi.chapter05.Source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author 影子
 * @create 2022-04-26-17:03
 **/
public class AppendQueryExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据源，并分配时间戳，生成水位线
        // 读取数据源，并分配时间戳、生成水位线
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                        new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                        new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                        new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                        new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
                ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //将数据流转换成表，并指定时间属性
        Table eventTable = tableEnv.fromDataStream(
                eventStream,
                $("user"),
                $("url"),
                $("timestamp").rowtime().as("ts")
                // 将 timestamp 指定为事件时间，并命名为 ts
        );

        //方便在SQL中引用，在环境中注册表EventTable
        tableEnv.createTemporaryView("EventTable", eventTable);

        //设置1小时滚动窗口，执行SQL统计查询
        Table result = tableEnv.sqlQuery(
                "SELECT " +
                        "user, " +
                        "window_end AS endT, " + // 窗口结束时间
                        "COUNT(url) AS cnt " + // 统计 url 访问次数
                        "FROM TABLE( " +
                        "TUMBLE( TABLE EventTable, " + // 1 小时滚动窗口
                        "DESCRIPTOR(ts), " +
                        "INTERVAL '1' HOUR)) " +
                        "GROUP BY user, window_start, window_end "
        );
        tableEnv.toDataStream(result).print();

        env.execute();
    }
}
