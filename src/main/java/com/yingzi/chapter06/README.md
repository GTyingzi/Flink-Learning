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
    ProcessLateDateExample：用process的方式处迟到数据理