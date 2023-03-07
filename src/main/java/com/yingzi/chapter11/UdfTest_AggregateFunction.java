package com.yingzi.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * @author 影子
 * @create 2022-04-27-17:01
 **/
public class UdfTest_AggregateFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.创建表
        String creatDDL = "CREATE TABLE clickTable (" +
                " `user` STRING, " +
                " url STRING, " +
                " ts BIGINT" +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'input/clicks.txt'," +
                " 'format' = 'csv'" +
                ")";
        tableEnv.executeSql(creatDDL);

        //2.注册自定义聚合函数
        tableEnv.createTemporarySystemFunction("WeightedAverage", WeightedAverage.class);

        //3.调用UDF进行查询转换
        Table resultTable = tableEnv.sqlQuery("select user,WeightedAverage(ts,1) as w_avg " +
                "from clickTable group by user");

        //4.转换成流打印输出
        tableEnv.toChangelogStream(resultTable).print();

        env.execute();
    }

    //单独定义一个累加器类型
    public static class WeightedAvgAccumulator {
        public long sum = 0;
        public int count = 0;
    }

    //实现自定义的聚合函数，计算加权平均值
    public static class WeightedAverage extends AggregateFunction<Long, WeightedAvgAccumulator> {

        @Override
        public Long getValue(WeightedAvgAccumulator accumulator) {
            if (accumulator.count == 0) return null;
            else return accumulator.sum / accumulator.count;
        }

        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }

        //累加计算方法
        public void accumulate(WeightedAvgAccumulator accmulator, Long iValue, Integer iWeight) {
            accmulator.sum += iValue * iWeight;
            accmulator.count += iWeight;
        }
    }
}
