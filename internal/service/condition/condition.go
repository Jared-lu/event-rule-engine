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
	// 使用 cel.DynType 声明一个占位变量，实际 activation 直接传 payload 扁平 map，
	// CEL 会从 activation 中按变量名查找。通过 cel.EnableMacroCallTracking 不需要，
	// 只需要 env 不开启强类型检查：使用 env.Parse 而非 env.Compile 跳过类型检查。
	env, err := cel.NewEnv()
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
	// 将 payload 扁平展开作为顶层变量，表达式可直接写 `coin > 0`
	out, _, err := c.prg.Eval(payload)
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
