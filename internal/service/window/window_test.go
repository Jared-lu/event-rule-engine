package window

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

// helpers
func ts(s string) int64 {
	t, err := time.ParseInLocation("2006-01-02 15:04:05", s, time.UTC)
	if err != nil {
		panic(err)
	}
	return t.Unix()
}

// --------------- GlobalWindow ---------------

func TestGlobalWindow_BucketKey(t *testing.T) {
	w := &GlobalWindow{}
	assert.Equal(t, "global", w.BucketKey(ts("2026-06-10 08:00:00")))
	assert.Equal(t, "global", w.BucketKey(0))
}

func TestGlobalWindow_ActiveKeys(t *testing.T) {
	w := &GlobalWindow{}
	keys := w.ActiveKeys(ts("2026-06-10 08:00:00"))
	assert.Equal(t, []string{"global"}, keys)
}

// --------------- FixedWindow ---------------

func TestFixedWindow_Month(t *testing.T) {
	w := &FixedWindow{Unit: "month"}
	assert.Equal(t, "2026-06", w.BucketKey(ts("2026-06-10 12:00:00")))
	assert.Equal(t, []string{"2026-06"}, w.ActiveKeys(ts("2026-06-10 12:00:00")))
}

func TestFixedWindow_Week(t *testing.T) {
	// 2026-06-10 is Wednesday, ISO week 24 of 2026
	w := &FixedWindow{Unit: "week"}
	assert.Equal(t, "2026-W24", w.BucketKey(ts("2026-06-10 12:00:00")))
	assert.Equal(t, []string{"2026-W24"}, w.ActiveKeys(ts("2026-06-10 12:00:00")))
}

func TestFixedWindow_Day(t *testing.T) {
	w := &FixedWindow{Unit: "day"}
	assert.Equal(t, "2026-06-10", w.BucketKey(ts("2026-06-10 23:59:59")))
	assert.Equal(t, []string{"2026-06-10"}, w.ActiveKeys(ts("2026-06-10 00:00:00")))
}

func TestFixedWindow_New(t *testing.T) {
	win, err := New(domain.WindowConfig{Type: "fixed", Unit: "day"})
	require.NoError(t, err)
	assert.Equal(t, "2026-06-10", win.BucketKey(ts("2026-06-10 06:00:00")))
}

// --------------- SlidingWindow ---------------

func TestSlidingWindow_BucketKey(t *testing.T) {
	w := &SlidingWindow{Days: 7}
	assert.Equal(t, "2026-06-10", w.BucketKey(ts("2026-06-10 23:00:00")))
}

func TestSlidingWindow_ActiveKeys_7d(t *testing.T) {
	w := &SlidingWindow{Days: 7}
	now := ts("2026-06-10 12:00:00")
	keys := w.ActiveKeys(now)
	require.Len(t, keys, 7)
	assert.Equal(t, "2026-06-04", keys[0])
	assert.Equal(t, "2026-06-10", keys[6])
}

func TestSlidingWindow_ActiveKeys_1d(t *testing.T) {
	w := &SlidingWindow{Days: 1}
	now := ts("2026-06-10 12:00:00")
	keys := w.ActiveKeys(now)
	require.Len(t, keys, 1)
	assert.Equal(t, "2026-06-10", keys[0])
}

func TestSlidingWindow_New(t *testing.T) {
	win, err := New(domain.WindowConfig{Type: "sliding", Size: "3d"})
	require.NoError(t, err)
	keys := win.ActiveKeys(ts("2026-06-10 00:00:00"))
	assert.Len(t, keys, 3)
}

func TestSlidingWindow_New_InvalidSize(t *testing.T) {
	_, err := New(domain.WindowConfig{Type: "sliding", Size: "3x"})
	assert.Error(t, err)
}

// --------------- RangeWindow ---------------

func TestRangeWindow_BucketKey_Inside(t *testing.T) {
	w := &RangeWindow{
		StartTime: ts("2026-06-01 00:00:00"),
		EndTime:   ts("2026-06-30 23:59:59"),
	}
	assert.Equal(t, "range", w.BucketKey(ts("2026-06-10 12:00:00")))
}

func TestRangeWindow_BucketKey_Outside(t *testing.T) {
	w := &RangeWindow{
		StartTime: ts("2026-06-01 00:00:00"),
		EndTime:   ts("2026-06-30 23:59:59"),
	}
	assert.Equal(t, "", w.BucketKey(ts("2026-07-01 00:00:00")))
	assert.Equal(t, "", w.BucketKey(ts("2026-05-31 23:59:59")))
}

func TestRangeWindow_BucketKey_Boundary(t *testing.T) {
	start := ts("2026-06-01 00:00:00")
	end := ts("2026-06-30 23:59:59")
	w := &RangeWindow{StartTime: start, EndTime: end}
	assert.Equal(t, "range", w.BucketKey(start))
	assert.Equal(t, "range", w.BucketKey(end))
}

func TestRangeWindow_ActiveKeys(t *testing.T) {
	w := &RangeWindow{
		StartTime: ts("2026-06-01 00:00:00"),
		EndTime:   ts("2026-06-30 23:59:59"),
	}
	assert.Equal(t, []string{"range"}, w.ActiveKeys(ts("2026-06-10 00:00:00")))
}

func TestRangeWindow_New(t *testing.T) {
	win, err := New(domain.WindowConfig{
		Type:      "range",
		StartTime: "2026-06-01 00:00:00",
		EndTime:   "2026-06-30 23:59:59",
	})
	require.NoError(t, err)
	assert.Equal(t, "range", win.BucketKey(ts("2026-06-15 00:00:00")))
}

// --------------- New unknown ---------------

func TestNew_Unknown(t *testing.T) {
	_, err := New(domain.WindowConfig{Type: "unknown"})
	assert.Error(t, err)
}
