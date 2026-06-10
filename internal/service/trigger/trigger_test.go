package trigger

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

func buckets(vals ...int64) []domain.Bucket {
	bs := make([]domain.Bucket, len(vals))
	for i, v := range vals {
		bs[i] = domain.Bucket{Key: "k", Value: v}
	}
	return bs
}

// --------------- EveryTrigger ---------------

func TestEveryTrigger_NotReached(t *testing.T) {
	e := &EveryTrigger{Step: 10}
	triggered, next := e.Check(buckets(5), domain.RuleProgress{})
	assert.False(t, triggered)
	assert.Equal(t, int64(10), next)
}

func TestEveryTrigger_ExactThreshold(t *testing.T) {
	e := &EveryTrigger{Step: 10}
	triggered, next := e.Check(buckets(10), domain.RuleProgress{})
	assert.True(t, triggered)
	assert.Equal(t, int64(20), next)
}

func TestEveryTrigger_OverThreshold(t *testing.T) {
	e := &EveryTrigger{Step: 10}
	triggered, next := e.Check(buckets(15), domain.RuleProgress{})
	assert.True(t, triggered)
	assert.Equal(t, int64(20), next)
}

func TestEveryTrigger_WithExistingNextThreshold(t *testing.T) {
	e := &EveryTrigger{Step: 10}
	prog := domain.RuleProgress{NextThreshold: 30}
	triggered, next := e.Check(buckets(25), prog)
	assert.False(t, triggered)
	assert.Equal(t, int64(30), next)
}

func TestEveryTrigger_ReachExistingNextThreshold(t *testing.T) {
	e := &EveryTrigger{Step: 10}
	prog := domain.RuleProgress{NextThreshold: 30}
	triggered, next := e.Check(buckets(30), prog)
	assert.True(t, triggered)
	assert.Equal(t, int64(40), next)
}

func TestEveryTrigger_MultiBuckets(t *testing.T) {
	e := &EveryTrigger{Step: 10}
	triggered, next := e.Check(buckets(4, 6), domain.RuleProgress{})
	assert.True(t, triggered)
	assert.Equal(t, int64(20), next)
}

// --------------- ThresholdTrigger ---------------

func TestThresholdTrigger_NotReached(t *testing.T) {
	tr := &ThresholdTrigger{Threshold: 100}
	triggered, next := tr.Check(buckets(50), domain.RuleProgress{})
	assert.False(t, triggered)
	assert.Equal(t, int64(100), next)
}

func TestThresholdTrigger_Exact(t *testing.T) {
	tr := &ThresholdTrigger{Threshold: 100}
	triggered, next := tr.Check(buckets(100), domain.RuleProgress{})
	assert.True(t, triggered)
	assert.Equal(t, int64(0), next)
}

func TestThresholdTrigger_Over(t *testing.T) {
	tr := &ThresholdTrigger{Threshold: 100}
	triggered, next := tr.Check(buckets(150), domain.RuleProgress{})
	assert.True(t, triggered)
	assert.Equal(t, int64(0), next)
}

func TestThresholdTrigger_AlreadyTriggered(t *testing.T) {
	tr := &ThresholdTrigger{Threshold: 100}
	prog := domain.RuleProgress{LastTriggeredAt: 1234567890}
	triggered, next := tr.Check(buckets(200), prog)
	assert.False(t, triggered)
	assert.Equal(t, int64(0), next)
}

// --------------- AllGteTrigger ---------------

func TestAllGteTrigger_AllPass(t *testing.T) {
	a := &AllGteTrigger{Threshold: 5}
	triggered, next := a.Check(buckets(5, 10, 7), domain.RuleProgress{})
	assert.True(t, triggered)
	assert.Equal(t, int64(5), next)
}

func TestAllGteTrigger_OneFail(t *testing.T) {
	a := &AllGteTrigger{Threshold: 5}
	triggered, next := a.Check(buckets(5, 4, 7), domain.RuleProgress{})
	assert.False(t, triggered)
	assert.Equal(t, int64(5), next)
}

func TestAllGteTrigger_Empty(t *testing.T) {
	a := &AllGteTrigger{Threshold: 5}
	triggered, _ := a.Check([]domain.Bucket{}, domain.RuleProgress{})
	assert.False(t, triggered)
}

func TestAllGteTrigger_Single(t *testing.T) {
	a := &AllGteTrigger{Threshold: 3}
	triggered, _ := a.Check(buckets(3), domain.RuleProgress{})
	assert.True(t, triggered)
}

// --------------- CountGteTrigger ---------------

func TestCountGteTrigger_Enough(t *testing.T) {
	c := &CountGteTrigger{Threshold: 5, Count: 3}
	triggered, next := c.Check(buckets(6, 5, 4, 7), domain.RuleProgress{})
	assert.True(t, triggered)
	assert.Equal(t, int64(5), next)
}

func TestCountGteTrigger_NotEnough(t *testing.T) {
	c := &CountGteTrigger{Threshold: 5, Count: 3}
	triggered, _ := c.Check(buckets(6, 4, 3), domain.RuleProgress{})
	assert.False(t, triggered)
}

func TestCountGteTrigger_Exact(t *testing.T) {
	c := &CountGteTrigger{Threshold: 5, Count: 2}
	triggered, _ := c.Check(buckets(5, 5), domain.RuleProgress{})
	assert.True(t, triggered)
}

func TestCountGteTrigger_Empty(t *testing.T) {
	c := &CountGteTrigger{Threshold: 5, Count: 1}
	triggered, _ := c.Check([]domain.Bucket{}, domain.RuleProgress{})
	assert.False(t, triggered)
}

// --------------- New factory ---------------

func TestNew_Every(t *testing.T) {
	tr, err := New(domain.TriggerConfig{Type: "every", Step: 10})
	require.NoError(t, err)
	triggered, _ := tr.Check(buckets(10), domain.RuleProgress{})
	assert.True(t, triggered)
}

func TestNew_Threshold(t *testing.T) {
	tr, err := New(domain.TriggerConfig{Type: "threshold", Threshold: 50})
	require.NoError(t, err)
	triggered, _ := tr.Check(buckets(50), domain.RuleProgress{})
	assert.True(t, triggered)
}

func TestNew_AllGte(t *testing.T) {
	tr, err := New(domain.TriggerConfig{Type: "all_gte", Threshold: 3})
	require.NoError(t, err)
	triggered, _ := tr.Check(buckets(3, 4), domain.RuleProgress{})
	assert.True(t, triggered)
}

func TestNew_CountGte(t *testing.T) {
	tr, err := New(domain.TriggerConfig{Type: "count_gte", Threshold: 3, Count: 2})
	require.NoError(t, err)
	triggered, _ := tr.Check(buckets(3, 4), domain.RuleProgress{})
	assert.True(t, triggered)
}

func TestNew_Unknown(t *testing.T) {
	_, err := New(domain.TriggerConfig{Type: "unknown"})
	assert.Error(t, err)
}
