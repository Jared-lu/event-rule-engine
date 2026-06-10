package aggregator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

// --------------- SumAggregator ---------------

func TestSumAggregator_Extract_Int64(t *testing.T) {
	a := &SumAggregator{Field: "coin"}
	v, err := a.Extract(map[string]interface{}{"coin": int64(100)})
	require.NoError(t, err)
	assert.Equal(t, int64(100), v)
}

func TestSumAggregator_Extract_Int(t *testing.T) {
	a := &SumAggregator{Field: "coin"}
	v, err := a.Extract(map[string]interface{}{"coin": int(50)})
	require.NoError(t, err)
	assert.Equal(t, int64(50), v)
}

func TestSumAggregator_Extract_Float64(t *testing.T) {
	a := &SumAggregator{Field: "amount"}
	v, err := a.Extract(map[string]interface{}{"amount": float64(3.9)})
	require.NoError(t, err)
	assert.Equal(t, int64(3), v)
}

func TestSumAggregator_Extract_Float32(t *testing.T) {
	a := &SumAggregator{Field: "amount"}
	v, err := a.Extract(map[string]interface{}{"amount": float32(7.1)})
	require.NoError(t, err)
	assert.Equal(t, int64(7), v)
}

func TestSumAggregator_Extract_FieldNotFound(t *testing.T) {
	a := &SumAggregator{Field: "missing"}
	_, err := a.Extract(map[string]interface{}{"coin": int64(1)})
	assert.Error(t, err)
}

func TestSumAggregator_Extract_UnsupportedType(t *testing.T) {
	a := &SumAggregator{Field: "coin"}
	_, err := a.Extract(map[string]interface{}{"coin": "hello"})
	assert.Error(t, err)
}

// --------------- CountAggregator ---------------

func TestCountAggregator_Extract(t *testing.T) {
	c := &CountAggregator{}
	v, err := c.Extract(nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), v)
}

func TestCountAggregator_Extract_IgnoresPayload(t *testing.T) {
	c := &CountAggregator{}
	v, err := c.Extract(map[string]interface{}{"coin": int64(999)})
	require.NoError(t, err)
	assert.Equal(t, int64(1), v)
}

// --------------- New factory ---------------

func TestNew_Sum(t *testing.T) {
	a, err := New(domain.AggregatorConfig{Type: "sum", Field: "coin"})
	require.NoError(t, err)
	v, err := a.Extract(map[string]interface{}{"coin": int64(5)})
	require.NoError(t, err)
	assert.Equal(t, int64(5), v)
}

func TestNew_Sum_MissingField(t *testing.T) {
	_, err := New(domain.AggregatorConfig{Type: "sum"})
	assert.Error(t, err)
}

func TestNew_Count(t *testing.T) {
	a, err := New(domain.AggregatorConfig{Type: "count"})
	require.NoError(t, err)
	v, err := a.Extract(nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), v)
}

func TestNew_Unknown(t *testing.T) {
	_, err := New(domain.AggregatorConfig{Type: "unknown"})
	assert.Error(t, err)
}
