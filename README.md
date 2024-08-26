代码在src/main/java/com/yingzi/


# wc
首先是入门的wordcount案例

```
wc/
    BatchWordCount：批处理
    StreamWordCount：无界流处理，从主机端口读取
    BoundedStreamWordCount：有界流处理，从文件中读取
```

# chapter05

 Source
 ```
    ClickSource：随机生成数据："Mary, ./home, 1000"
    SourceTest：从文件、集合、元素、socket、kafka中读取
    SourceCustomTest：实现自定义的并行SourceFunction
 ```
Transform
```
    TransformMapTest：Map案例
    TransformFlatMapTest：FlatMap案例
    TransformPartitionTest：Partition案例
    TransformRichFunctionTest：一个简单的RichFunction实现
    TransformFilterTest：Filter案例
    TransformReduceTest：Reduce案例
    TransformSimpleAggTest：简单的聚合案例
```

Sink
```
    SinkToFile：输出到本地
    SinkToEs：输出到lasticsearch
    SinkToHBase：输出到HBase
    SinkToKafka：输出到Kafka
    SinkToMySQL：输出到MySQL
    SinkToRedis：输出到Redis
```

# chapter06

```
    WatermarkTest：最简单的一个有序、乱序的水位线生成案例
    CustomWatermarkTest：自定义水位线策略
    EmitWatermarkInSourceFunction：数据源里面发送水位线
    WindowTest：增量聚合窗口案例
    WindowAggregateTest：AggregateFunction的一个案例
    WindowAggregateTest_PVUV：通过Aggregate计算pv、uv，得到平均用户活跃度
    WindowProcessTest：实现一个ProcessWindowFunction案例
    UvCountExample：使用AggregateFunction和ProcessWindowFunction计算UV
    UrlCountViewExample：使用AggregateFunction和ProcessWindowFunction计算网页点击量
    TriggerExample：自定义触发器
    LateDateTest：将迟到的数据放入侧输出流
    ProcessLateDateExample：用process的方式处理迟到数据
```

# chapter07

```
EventTimeTimerTest：基于自定义数据源基于事件时间的定时器触发例子
ProcessFunctionTest：处理函数的一个简单例子
ProcessingTimeTimerTest：定时器
TopNExample：TopN案例
TopNExample_ProcessAllWindowFunctions：全窗口实现TopN
```

# chapter08
```
IntervalJoinExample：intervalJoin的例子
UnionTest：流union的例子
ConnectTest：流connect的例子
CoGroupTest：流coGroup的例子
WindowJonTest：窗口join的例子
SplitStreamTest：利用侧输出流分流
BillCheckExample：实时检测两个不同来源的支付信息是否匹配
```

# chapter09
```
AverageTimestampExample：状态管理和聚合功能来实时计算和输出结果“计算每个用户的平均点击时间戳”
BroadcastStateExample：BroadcastState的例子
BufferingSinkExample：批量输出到Sink到例子
FakeWindowExample：利用KeyedProcessFunction实现滑动窗口统计，统计每个URL在连续10秒窗口内的PV
PeriodicPvExample：利用KeyedProcessFunction实现滑动窗口统计，统计每个用户在连续10秒窗口内的PV
TwoStreamFullJoinExample：两个数据流全连接的例子，具有相同的键
```

# chapter11
```
TableExample：Table API的简单例子
AppendQueryExample：利用Table API和SQL来执行基于滚动窗口的统计查询
CommonApiTest：基于老版本、blink版本的流批处理
TableToStreamExample：Table转Stream的例子
CumulateWindowExample：累加窗口的Table例子
TopNExample：TopN的Table例子
WindowTopNExample：窗口TopN的Table例子
UdfTest_TableFunction：注册自定义表函数
UdfTest_AggregateFunction：注册自定义聚合函数
UdfTest_ScalaarFunction：注册自定义标量函数
UdfTest_TableAggregareFunction：注册滚动自定义聚合函数
```


# chapter12
```
LoginFailDetectExample：使用where、next指定三次登陆失败CEP例子
LoginFailDetectProExample：使用times(3).consecutive()指定三次登陆失败的CEP例子
NFAExample：利用状态机的找到三次登陆失败的例子
OrderTimeoutDetectExample：利用CEP检测订单是否超时
```

