package com.yingzi.chapter12;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @author 影子
 * @create 2022-04-28-15:31
 **/
public class LoginFailDetectProExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1、获取登录事件流，并提取时间戳、生成水位线
        KeyedStream<LoginEvent, String> loginEventstream = env.fromElements(
                new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 8000L),
                new LoginEvent("user_2", "192.168.1.29", "success", 6000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                .withTimestampAssigner(
                        new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent loginEvent, long l) {
                                return loginEvent.timestamp;
                            }
                        }
                )
        ).keyBy(r -> r.userId);

        //2、定义模式，连续三次登录失败
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("fail") // 第一次登录失败事件
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                }).times(3).consecutive(); //指定严格紧邻的三次登录失败

        //3.将模式应用到数据流上，检测复杂事件
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventstream.keyBy(event -> event.userId), pattern);


        //4.将检测到的复杂事件提取出来，进行处理得到报警信息输出
        SingleOutputStreamOperator<String> warningStream = patternStream.process(new PatternProcessFunction<LoginEvent, String>() {
            @Override
            public void processMatch(Map<String, List<LoginEvent>> map, Context context, Collector<String> collector) throws Exception {
                // 提取三次登录失败事件
                LoginEvent firstFailEvent = map.get("fail").get(0);
                LoginEvent secondFailEvent = map.get("fail").get(1);
                LoginEvent thirdFailEvent = map.get("fail").get(2);

                collector.collect(firstFailEvent.userId + " 连续三次登录失败！ 登录时间 " +
                        firstFailEvent.timestamp + ", " +
                        secondFailEvent.timestamp + ", " +
                        thirdFailEvent.timestamp);
            }
        });

        //打印输出
        warningStream.print();

        env.execute();
    }
}
