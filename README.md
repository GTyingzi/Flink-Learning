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

