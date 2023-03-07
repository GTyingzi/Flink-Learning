package com.yingzi.chapter05.Sink;

import com.yingzi.chapter05.Source.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @author 影子
 * @create 2022-04-14-16:10
 **/
public class SinkToFile {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home", 3500L),
                new Event("Alice", "./prod?id=200", 3200L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Bob", "./prod?id=3", 4200L)
        );

        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(// 滚动策略
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024 * 1024 * 1024) // 文件大小达到1GB
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15)) // 包含15分钟的数据
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)) // 最近5分钟内没有收到新的数据
                                .build()
                )
                .build();

        stream.map(data -> data.toString()).addSink(streamingFileSink);

        env.execute();
    }
}
