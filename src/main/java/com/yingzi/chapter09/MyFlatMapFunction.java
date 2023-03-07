package com.yingzi.chapter09;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author 影子
 * @create 2022-04-21-22:27
 **/
public class MyFlatMapFunction extends RichFlatMapFunction<Long,String > {
    //声明状态
    private transient ValueState<Long> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        //在open生命周期方法中获取状态
        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<Long>(
                "my state", //状态名称
                Types.LONG //状态类型
        );
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Long value, Collector<String> out) throws Exception {
        //访问状态
        Long currentState = state.value();
        currentState += 1; //状态数值 + 1
        //更新状态
        state.update(currentState);
        if (currentState >= 100){
            out.collect("state: " + currentState);
            state.clear(); //清空状态
        }

    }
}
