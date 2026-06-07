package service

import (
	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

type Engine struct {
	// todo 很显然这个存在并发读写
	rules map[string][]Rule
}

func (e *Engine) RegisterRule(rule Rule) error {
	// 存储规则，既要保存在本地，也要保存在数据库。服务器启动的时候自动加载已有规则

	return nil
}

func (e *Engine) Consume(event domain.Event) {
	// todo 检查幂等
	// 消费完做幂等？还是先幂等再消费？

	// 拿出所有订阅的rule
	rules, ok := e.rules[event.Type]
	if !ok {

	}

	for _, rule := range rules {
		go e.match(event, rule)
	}

}

func (e *Engine) match(event domain.Event, rule Rule) {
	// 先判断是否满足条件
	if !rule.MatchCondition(event.Payload) {
		return
	}

	// 拿出时间维度

	// 取出历史数据

	// 聚合状态

	// 存储数据

	// 判断是否触发

	// 消息队列
}
