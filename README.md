# event-rule-engine
基于事件驱动的实时规则引擎平台

- event 业务方发送过来的事件
- rule 业务规则
  - event_type 业务规则订阅的事件类型
  - condition 定义一个事件需要满足的条件
  - window 时间窗口
  - aggregator 聚合器，负责计算进度，并落入到window指定的bucket中
  - trigger 根据window返回的buckets，判断当前用户是否命中的rule的要求


