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