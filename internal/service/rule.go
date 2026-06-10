package service

import (
	"github.com/Jared-lu/event-rule-engine/internal/domain"
	"github.com/Jared-lu/event-rule-engine/internal/service/aggregator"
	"github.com/Jared-lu/event-rule-engine/internal/service/condition"
	"github.com/Jared-lu/event-rule-engine/internal/service/trigger"
	"github.com/Jared-lu/event-rule-engine/internal/service/window"
)

// CompiledRule 编译后的规则，包含运行时组件
type CompiledRule struct {
	Rule       domain.Rule
	Condition  domain.Condition
	Window     domain.Window
	Aggregator domain.Aggregator
	Trigger    domain.Trigger
}

// Compile 将 Rule 配置编译为 CompiledRule
func Compile(rule domain.Rule) (CompiledRule, error) {
	cond, err := condition.New(rule.Condition)
	if err != nil {
		return CompiledRule{}, err
	}
	win, err := window.New(rule.Window)
	if err != nil {
		return CompiledRule{}, err
	}
	agg, err := aggregator.New(rule.Aggregator)
	if err != nil {
		return CompiledRule{}, err
	}
	trig, err := trigger.New(rule.Trigger)
	if err != nil {
		return CompiledRule{}, err
	}
	return CompiledRule{
		Rule:       rule,
		Condition:  cond,
		Window:     win,
		Aggregator: agg,
		Trigger:    trig,
	}, nil
}
