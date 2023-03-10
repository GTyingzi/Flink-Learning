package com.yingzi.chapter12;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 影子
 * @create 2022-04-28-19:43
 **/
public class NFAExample1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1、获取登录事件流，并提取时间戳、生成水位线
        KeyedStream<LoginEvent, String> loginEventstream = env.fromElements(
                new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
        )
                .keyBy(event -> event.userId);

        //2、数据按照顺序依次输入，用状态机进行处理，状态跳转
        SingleOutputStreamOperator<String> warningStream = loginEventstream.flatMap(new StateMachineMapper());

        env.execute();
    }

    //实现自定义的RichFlatMapFunction
    public static class StateMachineMapper extends RichFlatMapFunction<LoginEvent, String> {
        //声明状态机当前的状态
        ValueState<State> currentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            currentState = getRuntimeContext().getState(new ValueStateDescriptor<State>("state", State.class));
        }

        @Override
        public void flatMap(LoginEvent value, Collector<String> out) throws Exception {
            //如果状态为空，进行初始化
            State state = currentState.value();
            if (state == null) {
                state = State.Initial;
            }

            //调整到下一状态
            State nextState = state.transition(value.eventType);

            //  判断当前状态的特殊情况，直接进行跳转
            if (nextState == State.Matched) {
                //检测到了匹配，输出报警信息，不更新状态就是跳转会回S2
                out.collect(value.userId + "连续三次登录失败");
            } else if (nextState == State.Terminal) {
                //直接将状态更新为初始状态，重新开始检测
                currentState.update(State.Initial);
            } else {
                //状态覆盖跳转
                currentState.update(nextState);
            }
        }
    }

    //状态机实现
    public enum State {
        Terminal, //匹配失败，终止状态
        Matched,  //匹配成功

        // S2状态，传入基于S2状态可以进行一系列状态转移
        S2(new Transition("fail", Matched), new Transition("success", Terminal)),
        //  S1状态
        S1(new Transition("fail", S2), new Transition("success", Terminal)),
        //初始状态
        Initial(new Transition("fail", S1), new Transition("success", Terminal)),
        ;

        private Transition[] transitions;   //当前状态的转移规则

        State(Transition... transitions) {
            this.transitions = transitions;
        }

        //状态转移方法
        public State transition(String eventType) {
            for (Transition transition : transitions) {
                if (transition.getEventType().equals(eventType))
                    return transition.getTargetState();
            }

            //回到初始状态
            return Initial;
        }

    }

    //定义一个状态转移类，包含当前引起状态转移的事件类型，以及转移的目标状态
    public static class Transition {
        private String eventType;
        private State targetState;

        public Transition(String eventType, State targetState) {
            this.eventType = eventType;
            this.targetState = targetState;
        }

        public String getEventType() {
            return eventType;
        }

        public State getTargetState() {
            return targetState;
        }
    }
}
