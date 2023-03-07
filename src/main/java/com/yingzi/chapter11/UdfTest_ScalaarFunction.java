package com.yingzi.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author 影子
 * @create 2022-04-27-16:29
 **/
public class UdfTest_ScalaarFunction {

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

        //2.注册自定义标量函数
        tableEnv.createTemporarySystemFunction("MyHash",MyHashFunction.class);

        //3.调用UDF进行查询转换
        Table resultTable = tableEnv.sqlQuery("select user,MyHash(user) from clickTable");

        //4.转换成流打印输出
        tableEnv.toDataStream(resultTable).print();

        env.execute();
    }

    //自定义实现ScalarFunction
    public static class MyHashFunction extends ScalarFunction{
        public int eval(String str){
            return str.hashCode();
        }
    }
}
