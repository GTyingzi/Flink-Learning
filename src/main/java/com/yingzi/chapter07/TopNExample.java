package com.yingzi.chapter07;

import com.yingzi.chapter05.Source.ClickSource;
import com.yingzi.chapter05.Source.Event;
import com.yingzi.chapter06.UrlCountViewExample;
import com.yingzi.chapter06.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author 影子
 * @create 2022-04-18-14:01
 **/
public class TopNExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //读取数据
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        //1.按照url分组，统计窗口内每个url的访问量
        SingleOutputStreamOperator<UrlViewCount> urlCountStream = stream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))) //定义滑动窗口
                .aggregate(new UrlCountViewExample.UrlViewCountAgg(), new UrlCountViewExample.UrlViewCountResult());

        urlCountStream.print("url count");

        //2.对于同一窗口统计出的访问量，进行收集和排序
        urlCountStream.keyBy(data -> data.windowEnd)
                .process(new TopNProcessResult(2))
                .print();


        env.execute();
    }

    //实现自定义的KeyedProcessFunction
    public static class TopNProcessResult extends KeyedProcessFunction<Long, UrlViewCount, String> {
        //定义一个属性，n
        private Integer n;
        //定义列表状态
        private ListState<UrlViewCount> urlViewCountListState;

        public TopNProcessResult(Integer n) {
            this.n = n;
        }

        //在环境中获取状态


        @Override
        public void open(Configuration parameters) throws Exception {
            urlViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlViewCount>("url-count-list", Types.POJO(UrlViewCount.class))
            );
        }

        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
            //将数据保存到状态中
            urlViewCountListState.add(value);
            //注册windowEnd + 1ms的定时器
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();
            for (UrlViewCount urlViewCount : urlViewCountListState.get()) {
                urlViewCountArrayList.add(urlViewCount);
            }

            //排序
            urlViewCountArrayList.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return (int) (o2.count - o1.count);
                }
            });
            //包装信息打印输出
            StringBuilder result = new StringBuilder();
            result.append("--------------------------------\n");
            result.append("窗口结束时间：" + new Timestamp(ctx.getCurrentKey()) + "\n");

            //取List前两个，包装信息输出
            for (int i = 0; i < 2; i++) {
                UrlViewCount currTuple = urlViewCountArrayList.get(i);
                String info = "No. " + (i + 1) + " "
                        + "url: " + currTuple.url + " "
                        + "访问量" + currTuple.count + " \n";
                result.append(info);
            }
            result.append("--------------------------------\n");

            out.collect(result.toString());
        }
    }
}
