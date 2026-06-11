基于事件流的通用规则引擎平台。
# 背景
每个业务平台都会定义出很多业务规则，如果某个用户达成规则，就会执行特定动作。
比如每充值满100美元，就送出一个奖励。
但是各个业务规则散落在各个需求的代码里，缺乏统一管理和维护。这个平台就是为了解决这一个痛点：
统一接入和管理业务规则，并计算和保存每个用户对应每条规则的state；如果用户触发了某条规则，则由平台通知到业务方平台。


# 核心链路
                ┌─────────────┐
                │     Biz     │ 业务方
                └──────┬──────┘
                       │
                       │ Publish Event
                       ▼

                ┌─────────────┐
                │    Kafka    │
                └──────┬──────┘
                       │
                       ▼

                ┌─────────────┐
                │ Rule Engine │
                └──────┬──────┘
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼

     Window       Aggregate      Trigger

        └──────────────┬──────────────┘
                       ▼

                ┌─────────────┐
                │ State Store │
                └──────┬──────┘
                       │
                       ▼

                ┌─────────────┐
                │ Event Bus   │
                └──────┬──────┘
                       │ Publish Rule Event
                       ▼

                      Biz 业务方

# 组件
## Rule
一个Rule代表一条业务规则
type Rule struct{
    Biz string// 代表业务方
    ID  int64 // 业务规则唯一标识
    Name  string // 名称
    Description string // 描述
    EventType string // 事件类型
    Condition ConditionConfig // 事件需要满足的条件
    Window WindowConfig // 时间窗口
    Aggregator AggregatorConfig // 聚合器，负责计算规则进度
    Trigger TriggerConfig // 触发器，触发判断规则进度是否满足阈值
}
具体的业务规则配置参考 [rule_example.md](rule_example.md) 

## 业务方Event
代表一个业务方产生的业务消息
type Event struct{
    EID       int64                  
    Type      string // 事件类型          
    Biz       string                 
    UserId    int64               
    Timestamp int64                 
    Payload   map[string]interface{}
}

## Condition
Condition负责过滤事件，一个事件只有满足Condition所定义的条件才会进入rule engine然后被平台记录。
接口定义如下
type Condition interface{
    Match(event Event)bool
}

## Window
Window负责时间维度，决定一个事件产生的数据归属于哪一个桶。支持多种类型：
- global 全局
- fix 固定时间窗口，如每月，每周，每天
- slide 滑动时间窗口
- range 固定时间范围

Window会决定Aggregator计算的是哪一个桶的数据，以及Trigger能看到哪些桶的数据

## Aggregator
Aggregator负责接收特性的bucket数据，并根据配置的规则，更新规则进度

## Trigger
Trigger 负责接受若干Bucket的数据，并判断这些数据是否达到规则触发条件。如果触发规则，Trigger还需要计算出下一次触发的阈值，并返回给Store存储
type Trigger interface{
    Check(buckets []Buckets) Result
}

## Rule Engine
负责消费Event，根据Event找出订阅该EventType的规则，并进行匹配和计算。支持动态增减规则

## RuleRegistry
负责管理规则，所有的Rule都需要通过RuleRegistry才有效。

## State Store
负责存储每个业务方用户每一条规则的进度，根据Window的返回，取出桶的数据，并最终保管和更新Aggregate的计算结果。

## 平台Event
当一个Trigger返回bool时，说明当前的业务方触发了业务规则，这是需要将消息投递到消息队列告知业务方。
type Event strung{
    Biz string
    UserId int64
    RuleId int64
    CurrentValue int64
    Threshold int64
}

# 技术选型
消息队列：Kafka
数据库：Mysql + Gorm v2
分布式缓存：Redis
Web框架：Gin
Condition解析：CEL
