package com.yingzi.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author 影子
 * @create 2022-04-25-19:29
 **/
public class CommonApiTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.定义环境配置来创建表执行环境

        //基于blink版本planner进行流处理
        EnvironmentSettings settings1 = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        TableEnvironment tableEnv1 = TableEnvironment.create(settings1);

//        //1.1基于老版本planner进行流处理
//        EnvironmentSettings settings2 = EnvironmentSettings.newInstance()
//                .inStreamingMode()
//                .useOldPlanner()
//                .build();
//        TableEnvironment tableEnv2 = TableEnvironment.create(settings2);
//
//        //1.2基于老版本planner进行批处理
//        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
//
//        //1.3基于blink版本planner进行批处理
//        EnvironmentSettings settings3 = EnvironmentSettings.newInstance()
//                .inBatchMode()
//                .useBlinkPlanner()
//                .build();
//
//        TableEnvironment tableEnv3 = TableEnvironment.create(settings3);

        //2.创建表
        String creatDDL = "CREATE TABLE clickTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT" +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'input/clicks.txt'," +
                " 'format' = 'csv'" +
                ")";
        tableEnv1.executeSql(creatDDL);

        //调Table API进行表的查询转换
        Table clickTable = tableEnv1.from("clickTable");
        Table resultTable = clickTable.where($("user_name").isEqual("Bob"))
                .select($("user_name"), $("url"));
        tableEnv1.createTemporaryView("result2",resultTable);

        //执行SQL进行表的查询转换
        Table resultTable2 = tableEnv1.sqlQuery("select url,user_name from result2");

        // 执行聚合计算的查询转换

        //创建一张用于输出的表
        String creatOutDDL = "CREATE TABLE outTable (" +
                " url STRING, " +
                " user_name STRING " +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'output'," +
                " 'format' = 'csv'" +
                ")";
        tableEnv1.executeSql(creatOutDDL);

        //创建一张用于控制台打印输出的表
        String creatPrintOutDDL = "CREATE TABLE printOutTable (" +
                " url STRING, " +
                " user_name STRING " +
                ") WITH (" +
                " 'connector' = 'print'" +
                ")";
        tableEnv1.executeSql(creatPrintOutDDL);

        //输出表
        //resultTable.executeInsert("outTable");
//        resultTable2.executeInsert("printOutTable");

    }
}
