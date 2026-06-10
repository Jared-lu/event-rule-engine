package condition

import (
	"fmt"

	"github.com/google/cel-go/cel"
	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

// CELCondition 使用 CEL 表达式过滤事件
type CELCondition struct {
	prg cel.Program
}

func New(cfg domain.ConditionConfig) (domain.Condition, error) {
	if cfg.Expression == "" {
		return &alwaysTrue{}, nil
	}
	env, err := cel.NewEnv(
		cel.Variable("payload", cel.MapType(cel.StringType, cel.DynType)),
	)
	if err != nil {
		return nil, fmt.Errorf("condition: cel env: %w", err)
	}
	ast, issues := env.Parse(cfg.Expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("condition: parse %q: %w", cfg.Expression, issues.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("condition: program: %w", err)
	}
	return &CELCondition{prg: prg}, nil
}

func (c *CELCondition) Match(payload map[string]interface{}) (bool, error) {
	out, _, err := c.prg.Eval(map[string]interface{}{
		"payload": payload,
	})
	if err != nil {
		return false, fmt.Errorf("condition: eval: %w", err)
	}
	result, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("condition: expression did not return bool")
	}
	return result, nil
}

// alwaysTrue 空表达式时直接放行
type alwaysTrue struct{}

func (a *alwaysTrue) Match(_ map[string]interface{}) (bool, error) {
	return true, nil
}
