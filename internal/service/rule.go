package service

type Rule struct {
	condition  string
	aggregator string
	tigger     string
	window     string
}

func (r *Rule) MatchCondition(payload map[string]interface{}) bool {
	return true
}

func (r *Rule) Aggregate(payload map[string]interface{}) int64 {
	return 0
}

func (r *Rule) Trigger(payload map[string]interface{}) bool {
	return true
}
