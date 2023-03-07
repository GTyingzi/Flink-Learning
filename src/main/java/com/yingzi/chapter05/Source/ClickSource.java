package com.yingzi.chapter05.Source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @author 影子
 * @create 2022-04-12-17:37
 **/
public class ClickSource implements SourceFunction<Event> {
    //随机生成数据
    Random random = new Random();
    //定义字段选取的数据集
    String[] users = {"Mary","Alice","Bob","Cary"};
    String[] urls = {"./home","./cart","./fav","./prod?id=100"};

    //声明一个标志位
    private Boolean running = true;

    @Override
    public void run(SourceContext ctx) throws Exception {
        //循环生成数据
        while (running){
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long timestamp = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new Event(user,url,timestamp));

            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
