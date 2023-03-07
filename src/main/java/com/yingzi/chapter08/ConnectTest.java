package com.yingzi.chapter08;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author 影子
 * @create 2022-04-19-15:31
 **/
public class ConnectTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3);
        DataStreamSource<Long> stream2 = env.fromElements(4L, 5L, 6L, 7L);

        stream2.connect(stream1)
                .map(new CoMapFunction<Long, Integer, String >() {
                    @Override
                    public String map1(Long value) throws Exception {
                        return "Long：" + value.toString();
                    }

                    @Override
                    public String map2(Integer value) throws Exception {
                        return "Integer：" + value.toString();
                    }
                }).print();

        env.execute();
    }
}
