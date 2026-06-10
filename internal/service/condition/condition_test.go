package condition

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

func mustNew(t *testing.T, expr string) *CELCondition {
	t.Helper()
	c, err := New(domain.ConditionConfig{Expression: expr})
	require.NoError(t, err)
	cel, ok := c.(*CELCondition)
	require.True(t, ok, "expected *CELCondition")
	return cel
}

// --------------- empty expression -> alwaysTrue ---------------

func TestNew_EmptyExpression_AlwaysTrue(t *testing.T) {
	c, err := New(domain.ConditionConfig{Expression: ""})
	require.NoError(t, err)
	ok, err := c.Match(map[string]interface{}{})
	require.NoError(t, err)
	assert.True(t, ok)
}

// --------------- coin > 0 ---------------

func TestCEL_CoinGtZero_True(t *testing.T) {
	c := mustNew(t, "coin > 0")
	ok, err := c.Match(map[string]interface{}{"coin": int64(5)})
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestCEL_CoinGtZero_False(t *testing.T) {
	c := mustNew(t, "coin > 0")
	ok, err := c.Match(map[string]interface{}{"coin": int64(0)})
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestCEL_CoinGtZero_Negative(t *testing.T) {
	c := mustNew(t, "coin > 0")
	ok, err := c.Match(map[string]interface{}{"coin": int64(-1)})
	require.NoError(t, err)
	assert.False(t, ok)
}

// --------------- gift_id == 1001 ---------------

func TestCEL_GiftIdEq_True(t *testing.T) {
	c := mustNew(t, "gift_id == 1001")
	ok, err := c.Match(map[string]interface{}{"gift_id": int64(1001)})
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestCEL_GiftIdEq_False(t *testing.T) {
	c := mustNew(t, "gift_id == 1001")
	ok, err := c.Match(map[string]interface{}{"gift_id": int64(1002)})
	require.NoError(t, err)
	assert.False(t, ok)
}

// --------------- followed == true ---------------

func TestCEL_FollowedTrue(t *testing.T) {
	c := mustNew(t, "followed == true")
	ok, err := c.Match(map[string]interface{}{"followed": true})
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestCEL_FollowedFalse(t *testing.T) {
	c := mustNew(t, "followed == true")
	ok, err := c.Match(map[string]interface{}{"followed": false})
	require.NoError(t, err)
	assert.False(t, ok)
}

// --------------- compound expressions ---------------

func TestCEL_Compound_And(t *testing.T) {
	c := mustNew(t, "coin > 0 && followed == true")
	ok, err := c.Match(map[string]interface{}{"coin": int64(10), "followed": true})
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = c.Match(map[string]interface{}{"coin": int64(10), "followed": false})
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestCEL_Compound_Or(t *testing.T) {
	c := mustNew(t, "coin > 0 || followed == true")
	ok, err := c.Match(map[string]interface{}{"coin": int64(0), "followed": true})
	require.NoError(t, err)
	assert.True(t, ok)
}

// --------------- invalid expression ---------------

func TestNew_InvalidExpression(t *testing.T) {
	_, err := New(domain.ConditionConfig{Expression: "coin >>>> 0 !!!"})
	assert.Error(t, err)
}
