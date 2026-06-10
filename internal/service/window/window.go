package window

import (
	"fmt"
	"strconv"
	"time"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

// GlobalWindow 全局窗口，所有事件归属同一个 bucket
type GlobalWindow struct{}

func (g *GlobalWindow) BucketKey(_ int64) string {
	return "global"
}

func (g *GlobalWindow) ActiveKeys(_ int64) []string {
	return []string{"global"}
}

// FixedWindow 固定时间窗口：month | week | day
type FixedWindow struct {
	Unit string // month | week | day
}

func (f *FixedWindow) BucketKey(eventTime int64) string {
	t := time.Unix(eventTime, 0).UTC()
	return fixedKey(t, f.Unit)
}

func (f *FixedWindow) ActiveKeys(now int64) []string {
	t := time.Unix(now, 0).UTC()
	return []string{fixedKey(t, f.Unit)}
}

func fixedKey(t time.Time, unit string) string {
	switch unit {
	case "month":
		return t.Format("2006-01")
	case "week":
		year, week := t.ISOWeek()
		return fmt.Sprintf("%d-W%02d", year, week)
	default: // day
		return t.Format("2006-01-02")
	}
}

// SlidingWindow 滑动时间窗口，按天 bucket，维护最近 N 天
type SlidingWindow struct {
	Days int // size 解析出来的天数
}

func (s *SlidingWindow) BucketKey(eventTime int64) string {
	return time.Unix(eventTime, 0).UTC().Format("2006-01-02")
}

func (s *SlidingWindow) ActiveKeys(now int64) []string {
	t := time.Unix(now, 0).UTC()
	keys := make([]string, s.Days)
	for i := 0; i < s.Days; i++ {
		keys[i] = t.AddDate(0, 0, -(s.Days - 1 - i)).Format("2006-01-02")
	}
	return keys
}

// RangeWindow 固定时间范围窗口
type RangeWindow struct {
	StartTime int64 // unix timestamp
	EndTime   int64
}

func (r *RangeWindow) BucketKey(eventTime int64) string {
	if eventTime >= r.StartTime && eventTime <= r.EndTime {
		return "range"
	}
	return "" // 不在范围内，返回空串由 Engine 过滤
}

func (r *RangeWindow) ActiveKeys(_ int64) []string {
	return []string{"range"}
}

// New 根据 WindowConfig 构建 Window 实现
func New(cfg domain.WindowConfig) (domain.Window, error) {
	switch cfg.Type {
	case "global":
		return &GlobalWindow{}, nil
	case "fixed":
		return &FixedWindow{Unit: cfg.Unit}, nil
	case "sliding":
		days, err := parseDays(cfg.Size)
		if err != nil {
			return nil, fmt.Errorf("window: invalid sliding size %q: %w", cfg.Size, err)
		}
		return &SlidingWindow{Days: days}, nil
	case "range":
		start, err := parseTime(cfg.StartTime)
		if err != nil {
			return nil, fmt.Errorf("window: invalid start_time: %w", err)
		}
		end, err := parseTime(cfg.EndTime)
		if err != nil {
			return nil, fmt.Errorf("window: invalid end_time: %w", err)
		}
		return &RangeWindow{StartTime: start, EndTime: end}, nil
	default:
		return nil, fmt.Errorf("window: unknown type %q", cfg.Type)
	}
}

// parseDays 解析形如 "7d" 的字符串，返回天数
func parseDays(s string) (int, error) {
	if len(s) < 2 || s[len(s)-1] != 'd' {
		return 0, fmt.Errorf("expected format Nd, got %q", s)
	}
	n, err := strconv.Atoi(s[:len(s)-1])
	if err != nil {
		return 0, err
	}
	return n, nil
}

func parseTime(s string) (int64, error) {
	t, err := time.ParseInLocation("2006-01-02 15:04:05", s, time.UTC)
	if err != nil {
		return 0, err
	}
	return t.Unix(), nil
}
