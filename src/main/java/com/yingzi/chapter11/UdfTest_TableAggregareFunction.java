package com.yingzi.chapter11;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;


/**
 * @author 影子
 * @create 2022-04-27-17:19
 **/
public class UdfTest_TableAggregareFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.创建表
        String creatDDL = "CREATE TABLE clickTable (" +
                " `user` STRING, " +
                " url STRING, " +
                " ts BIGINT, " +
                " et AS TO_TIMESTAMP( FROM_UNIXTIME(ts /1000), " +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'input/clicks.txt'," +
                " 'format' = 'csv'" +
                ")";
        tableEnv.executeSql(creatDDL);

        //2.注册自定义聚合函数
        tableEnv.createTemporarySystemFunction("Top2", Top2.class);

        //3.调用UDF进行查询转换

        String windowAggreQuery = "SELECT user,COUNT(url) AS cnt,window_start,window_end " +
                "FROM TABLE (" +
                " TUMBLE(TABLE clickTable,DESCRIPTIOR(et),INTERVAL '10' SECOND)" +
                ")" +
                "GROUP BY user,window_start,window_end";
        Table aggTable = tableEnv.sqlQuery(windowAggreQuery);

        Table resultTable = aggTable.groupBy($("window_end"))
                .flatAggregate(call("Top2", $("cnt")).as("value", "rank"))
                .select($("window_end"), $("value"), $("rank"));

        //4.转换成流打印输出
        tableEnv.toChangelogStream(resultTable).print();

        env.execute();
    }

    //单独定义一个累加器类型，包含了当前最大、第二大的数据
    public static class Top2Accumulator {
        public Long max;
        public Long secondMax;
    }

    //实现一个自定义的表聚合函数
    public static class Top2 extends TableAggregateFunction<Tuple2<Long, Integer>, Top2Accumulator> {

        @Override
        public Top2Accumulator createAccumulator() {
            Top2Accumulator top2Accumulator = new Top2Accumulator();
            top2Accumulator.max = Long.MAX_VALUE;
            top2Accumulator.secondMax = Long.MIN_VALUE;
            return top2Accumulator;
        }

        // 定义一个更新累加器的方法
        public void accumulate(Top2Accumulator accumulator, Long value) {
            if (value > accumulator.max) {
                accumulator.secondMax = accumulator.max;
                accumulator.max = value;
            } else if (value > accumulator.secondMax) {
                accumulator.secondMax = value;
            }
        }

        //输出结果，当前的top2
        public void emitValue(Top2Accumulator accumulator, Collector<Tuple2<Long, Integer>> out) {
            if (accumulator.max != Long.MIN_VALUE) {
                out.collect(Tuple2.of(accumulator.max, 1));
            }
            if (accumulator.secondMax != Long.MIN_VALUE) {
                out.collect(Tuple2.of(accumulator.secondMax, 2));
            }
        }
    }

}
