1. 累计送礼金币，每满100万金币升1级
配置
```json
{
  "rule_id": 1001,
  "name": "送礼达人",

  "event_type": "gift_send",

  "condition": {
    "expression": "coin > 0"
  },

  "window": {
    "type": "global"
  },

  "aggregate": {
    "type": "sum",
    "field": "coin"
  },

  "trigger": {
    "type": "every",
    "step": 1000000
  }
}
```

效果：
```text
累计金币:
500000  -> Lv0
1000000 -> Lv1
2500000 -> Lv2
3100000 -> Lv3
```

2. 送出指定礼物(火箭)，每送100个升1级
配置：
```json
{
  "rule_id": 1002,
  "name": "火箭大师",

  "event_type": "gift_send",

  "condition": {
    "expression": "gift_id == 1001"
  },

  "window": {
    "type": "global"
  },

  "aggregate": {
    "type": "count"
  },

  "trigger": {
    "type": "every",
    "step": 100
  }
}
```
效果：
```text
送出火箭:

99个  -> Lv0

100个 -> Lv1

250个 -> Lv2

399个 -> Lv3
```

3. 连续3天，每天充值金额>=500美元
配置：
```json
{
  "rule_id": 1003,
  "name": "连续充值达人",

  "event_type": "recharge",

  "condition": {
    "expression": "amount > 0"
  },

  "window": {
    "type": "sliding",
    "size": "3d",
    "bucket": "day"
  },

  "aggregate": {
    "type": "sum",
    "field": "amount"
  },

  "trigger": {
    "type": "all_gte",
    "threshold": 500
  }
}
```
Bucket数据：
```json
{
"2026-05-09": 699,
"2026-05-10": 700,
"2026-05-11": 600
}
```

Window：
```json
[
"2026-05-09",
"2026-05-10",
"2026-05-11"
]
```

Trigger：
```text
699 >= 500
700 >= 500
600 >= 500
```
结果：
Triggered=true

4. 每月送礼次数达到100次
配置：
```json
{
  "rule_id": 1004,
  "name": "月度活跃",

  "event_type": "gift_send",

  "condition": {
    "expression": "coin > 0"
  },

  "window": {
    "type": "fixed",
    "unit": "month"
  },

  "aggregate": {
    "type": "count"
  },

  "trigger": {
    "type": "threshold",
    "threshold": 100
  }
}
```
Bucket：
```json
{
  "2026-05": 87
}

```
达到：
```json
{
  "2026-05": 87
}
```
触发。

5. 最近7天至少有5天获得20个以上关注
配置：
```json
{
  "rule_id": 1005,
  "name": "社交达人",

  "event_type": "follow",

  "condition": {
    "expression": "followed == true"
  },

  "window": {
    "type": "sliding",
    "size": "7d",
    "bucket": "day"
  },

  "aggregate": {
    "type": "count"
  },

  "trigger": {
    "type": "count_gte",
    "threshold": 20,
    "count": 5
  }
}
```

Bucket:
```json
{
  "05-05": 30,
  "05-06": 25,
  "05-07": 10,
  "05-08": 21,
  "05-09": 22,
  "05-10": 23,
  "05-11": 5
}
```
最近7天：
```text
>=20 的天数
05-05 ✓
05-06 ✓
05-08 ✓
05-09 ✓
05-10 ✓
```
共5天，触发。

6. 活动期间，累计消费金币 >= 100000
活动时间：2026-05-01~2026-05-31

配置：
```json
{
  "rule_id": 1006,
  "name": "活动消费达人",

  "event_type": "gift_send",

  "condition": {
    "expression": "coin > 0"
  },

  "window": {
    "type": "range",
    "start_time": "2026-05-01 00:00:00",
    "end_time": "2026-05-31 23:59:59"
  },

  "aggregate": {
    "type": "sum",
    "field": "coin"
  },

  "trigger": {
    "type": "threshold",
    "threshold": 100000
  }
}
```