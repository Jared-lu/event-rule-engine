package integration_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	redisclient "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
	"github.com/Jared-lu/event-rule-engine/internal/repository"
	"github.com/Jared-lu/event-rule-engine/internal/service"
	"github.com/Jared-lu/event-rule-engine/internal/store"
)

// ---- mock EventBus ----

type mockEventBus struct {
	mu     sync.Mutex
	events []domain.RuleEvent
}

func (m *mockEventBus) Publish(_ context.Context, e domain.RuleEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e)
	return nil
}

func (m *mockEventBus) Events() []domain.RuleEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]domain.RuleEvent, len(m.events))
	copy(cp, m.events)
	return cp
}

// ---- test infra ----

type testEnv struct {
	db  *gorm.DB
	rdb *redisclient.Client
}

func setupTestEnv(t *testing.T) *testEnv {
	t.Helper()
	ctx := context.Background()

	// Try env-var based connection first (CI / no-Docker fallback)
	mysqlDSN := os.Getenv("TEST_MYSQL_DSN")
	redisAddr := os.Getenv("TEST_REDIS_ADDR")

	if mysqlDSN == "" && redisAddr == "" {
		// Use testcontainers
		mysqlC, err := tcmysql.Run(ctx,
			"mysql:8.0",
			tcmysql.WithDatabase("testdb"),
			tcmysql.WithUsername("root"),
			tcmysql.WithPassword("password"),
		)
		if err != nil {
			t.Skipf("testcontainers MySQL unavailable: %v", err)
		}
		t.Cleanup(func() { _ = mysqlC.Terminate(ctx) })

		host, err := mysqlC.Host(ctx)
		require.NoError(t, err)
		port, err := mysqlC.MappedPort(ctx, "3306")
		require.NoError(t, err)
		mysqlDSN = fmt.Sprintf("root:password@tcp(%s:%s)/testdb?charset=utf8mb4&parseTime=True&loc=UTC", host, port.Port())

		redisC, err := tcredis.Run(ctx, "redis:7")
		if err != nil {
			t.Skipf("testcontainers Redis unavailable: %v", err)
		}
		t.Cleanup(func() { _ = redisC.Terminate(ctx) })

		redisAddr, err = redisC.ConnectionString(ctx)
		require.NoError(t, err)
		// ConnectionString returns "redis://localhost:PORT" — strip scheme
		if len(redisAddr) > 8 && redisAddr[:8] == "redis://" {
			redisAddr = redisAddr[8:]
		}
	} else if mysqlDSN == "" || redisAddr == "" {
		t.Skip("set both TEST_MYSQL_DSN and TEST_REDIS_ADDR to use external DB")
	}

	db, err := gorm.Open(mysql.Open(mysqlDSN), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	require.NoError(t, store.AutoMigrate(db))

	rdb := redisclient.NewClient(&redisclient.Options{Addr: redisAddr})
	require.NoError(t, rdb.Ping(ctx).Err())

	return &testEnv{db: db, rdb: rdb}
}

// buildEngine creates a fresh Engine + StateStore + mocked EventBus.
// Each test should call this to get isolated components (but they share the
// same DB/Redis — use distinct userIDs per rule to avoid cross-pollution).
func (e *testEnv) buildEngine(t *testing.T) (*service.Engine, *service.RuleRegistry, *mockEventBus) {
	t.Helper()
	bus := &mockEventBus{}
	st := store.NewStateStore(e.db, e.rdb)
	idempotency := store.NewRedisIdempotency(e.rdb)

	ruleDAO := repository.NewRuleDAO(e.db)
	require.NoError(t, ruleDAO.AutoMigrate())
	ruleRepo := repository.NewRuleRepository(ruleDAO)
	registry, err := service.NewRuleRegistry(context.Background(), ruleRepo)
	require.NoError(t, err)

	eng := service.NewEngine(registry, st, bus, idempotency)
	return eng, registry, bus
}

// waitTriggered polls until at least minCount events for the given ruleID appear,
// or times out.
func waitTriggered(t *testing.T, bus *mockEventBus, ruleID int64, minCount int, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		count := 0
		for _, ev := range bus.Events() {
			if ev.RuleID == ruleID {
				count++
			}
		}
		return count >= minCount
	}, timeout, 20*time.Millisecond)
}

// waitNotTriggered asserts that NO event for ruleID appears within the window.
func waitNotTriggered(t *testing.T, bus *mockEventBus, ruleID int64, window time.Duration) {
	t.Helper()
	time.Sleep(window)
	for _, ev := range bus.Events() {
		if ev.RuleID == ruleID {
			t.Fatalf("rule %d unexpectedly triggered", ruleID)
		}
	}
}

// eid generator — simple monotonic counter, thread-safe per test.
var eidCounter int64
var eidMu sync.Mutex

func nextEID() int64 {
	eidMu.Lock()
	defer eidMu.Unlock()
	eidCounter++
	return eidCounter
}

// ---- Rule 1: 累计送礼金币，每满100万升1级 ----

func TestRule1_GiftCoin_EveryMillion(t *testing.T) {
	env := setupTestEnv(t)
	eng, registry, bus := env.buildEngine(t)
	ctx := context.Background()

	const (
		biz    = "test"
		userID = int64(1001)
		ruleID = int64(1001)
	)

	rule := domain.Rule{
		Biz:       biz,
		ID:        ruleID,
		Name:      "送礼达人",
		EventType: "gift_send",
		Condition: domain.ConditionConfig{Expression: "coin > 0"},
		Window:    domain.WindowConfig{Type: "global"},
		Aggregator: domain.AggregatorConfig{Type: "sum", Field: "coin"},
		Trigger:   domain.TriggerConfig{Type: "every", Step: 1000000},
	}
	require.NoError(t, registry.Register(ctx, rule))

	now := time.Now().Unix()

	// Send 500000 coins — should NOT trigger
	eng.Consume(ctx, domain.Event{
		EID: nextEID(), Type: "gift_send", Biz: biz, UserId: userID,
		Timestamp: now, Payload: map[string]interface{}{"coin": int64(500000)},
	})
	waitNotTriggered(t, bus, ruleID, 100*time.Millisecond)

	// Send another 500000 → total 1_000_000 → trigger level 1
	eng.Consume(ctx, domain.Event{
		EID: nextEID(), Type: "gift_send", Biz: biz, UserId: userID,
		Timestamp: now, Payload: map[string]interface{}{"coin": int64(500000)},
	})
	waitTriggered(t, bus, ruleID, 1, 3*time.Second)

	// Send 1_500_000 → total 2_500_000 → crosses 2_000_000 threshold → trigger level 2
	eng.Consume(ctx, domain.Event{
		EID: nextEID(), Type: "gift_send", Biz: biz, UserId: userID,
		Timestamp: now, Payload: map[string]interface{}{"coin": int64(1500000)},
	})
	waitTriggered(t, bus, ruleID, 2, 3*time.Second)

	st := store.NewStateStore(env.db, env.rdb)
	prog, err := st.GetProgress(ctx, biz, userID, ruleID)
	require.NoError(t, err)
	require.Equal(t, int64(2), prog.CurrentLevel)
	require.Equal(t, int64(3000000), prog.NextThreshold)
}

// ---- Rule 2: 送出礼物(gift_id==1001)，每100个升1级 ----

func TestRule2_GiftCount_Every100(t *testing.T) {
	env := setupTestEnv(t)
	eng, registry, bus := env.buildEngine(t)
	ctx := context.Background()

	const (
		biz    = "test"
		userID = int64(1002)
		ruleID = int64(1002)
	)

	rule := domain.Rule{
		Biz:       biz,
		ID:        ruleID,
		Name:      "火箭大师",
		EventType: "gift_send",
		Condition: domain.ConditionConfig{Expression: "gift_id == 1001"},
		Window:    domain.WindowConfig{Type: "global"},
		Aggregator: domain.AggregatorConfig{Type: "count"},
		Trigger:   domain.TriggerConfig{Type: "every", Step: 100},
	}
	require.NoError(t, registry.Register(ctx, rule))

	now := time.Now().Unix()

	// Send 99 gifts — should NOT trigger
	for i := 0; i < 99; i++ {
		eng.Consume(ctx, domain.Event{
			EID: nextEID(), Type: "gift_send", Biz: biz, UserId: userID,
			Timestamp: now,
			Payload:   map[string]interface{}{"gift_id": int64(1001), "coin": int64(100)},
		})
	}
	waitNotTriggered(t, bus, ruleID, 150*time.Millisecond)

	// Send the 100th — should trigger level 1
	eng.Consume(ctx, domain.Event{
		EID: nextEID(), Type: "gift_send", Biz: biz, UserId: userID,
		Timestamp: now,
		Payload:   map[string]interface{}{"gift_id": int64(1001), "coin": int64(100)},
	})
	waitTriggered(t, bus, ruleID, 1, 3*time.Second)

	st := store.NewStateStore(env.db, env.rdb)
	prog, err := st.GetProgress(ctx, biz, userID, ruleID)
	require.NoError(t, err)
	require.Equal(t, int64(1), prog.CurrentLevel)
}

// ---- Rule 3: 连续3天每天充值>=500 ----

func TestRule3_Sliding3d_AllGte500(t *testing.T) {
	env := setupTestEnv(t)
	eng, registry, bus := env.buildEngine(t)
	ctx := context.Background()

	const (
		biz    = "test"
		userID = int64(1003)
		ruleID = int64(1003)
	)

	rule := domain.Rule{
		Biz:       biz,
		ID:        ruleID,
		Name:      "连续充值达人",
		EventType: "recharge",
		Condition: domain.ConditionConfig{Expression: "amount > 0"},
		Window:    domain.WindowConfig{Type: "sliding", Size: "3d", BucketUnit: "day"},
		Aggregator: domain.AggregatorConfig{Type: "sum", Field: "amount"},
		Trigger:   domain.TriggerConfig{Type: "all_gte", Threshold: 500},
	}
	require.NoError(t, registry.Register(ctx, rule))

	// Use timestamps that produce 3 consecutive days in the recent past.
	// We pick days D-2, D-1, D so that ActiveKeys(now) covers all three.
	base := time.Now().UTC().Truncate(24 * time.Hour)
	day0 := base.AddDate(0, 0, -2).Unix()
	day1 := base.AddDate(0, 0, -1).Unix()
	day2 := base.Unix()

	eng.Consume(ctx, domain.Event{
		EID: nextEID(), Type: "recharge", Biz: biz, UserId: userID,
		Timestamp: day0, Payload: map[string]interface{}{"amount": int64(699)},
	})
	eng.Consume(ctx, domain.Event{
		EID: nextEID(), Type: "recharge", Biz: biz, UserId: userID,
		Timestamp: day1, Payload: map[string]interface{}{"amount": int64(700)},
	})
	eng.Consume(ctx, domain.Event{
		EID: nextEID(), Type: "recharge", Biz: biz, UserId: userID,
		Timestamp: day2, Payload: map[string]interface{}{"amount": int64(600)},
	})

	waitTriggered(t, bus, ruleID, 1, 3*time.Second)

	st := store.NewStateStore(env.db, env.rdb)
	keys := []string{
		base.AddDate(0, 0, -2).Format("2006-01-02"),
		base.AddDate(0, 0, -1).Format("2006-01-02"),
		base.Format("2006-01-02"),
	}
	buckets, err := st.GetBuckets(ctx, biz, userID, ruleID, keys)
	require.NoError(t, err)
	for _, b := range buckets {
		require.GreaterOrEqual(t, b.Value, int64(500), "bucket %s should be >= 500", b.Key)
	}
}

// ---- Rule 4: 每月送礼次数达到100次 ----

func TestRule4_FixedMonth_Threshold100(t *testing.T) {
	env := setupTestEnv(t)
	eng, registry, bus := env.buildEngine(t)
	ctx := context.Background()

	const (
		biz    = "test"
		userID = int64(1004)
		ruleID = int64(1004)
	)

	rule := domain.Rule{
		Biz:       biz,
		ID:        ruleID,
		Name:      "月度活跃",
		EventType: "gift_send",
		Condition: domain.ConditionConfig{Expression: "coin > 0"},
		Window:    domain.WindowConfig{Type: "fixed", Unit: "month"},
		Aggregator: domain.AggregatorConfig{Type: "count"},
		Trigger:   domain.TriggerConfig{Type: "threshold", Threshold: 100},
	}
	require.NoError(t, registry.Register(ctx, rule))

	now := time.Now().Unix()

	// Send 99 — should NOT trigger
	for i := 0; i < 99; i++ {
		eng.Consume(ctx, domain.Event{
			EID: nextEID(), Type: "gift_send", Biz: biz, UserId: userID,
			Timestamp: now, Payload: map[string]interface{}{"coin": int64(10)},
		})
	}
	waitNotTriggered(t, bus, ruleID, 200*time.Millisecond)

	// Send the 100th — should trigger
	eng.Consume(ctx, domain.Event{
		EID: nextEID(), Type: "gift_send", Biz: biz, UserId: userID,
		Timestamp: now, Payload: map[string]interface{}{"coin": int64(10)},
	})
	waitTriggered(t, bus, ruleID, 1, 3*time.Second)
}

// ---- Rule 5: 最近7天至少5天获得20个以上关注 ----

func TestRule5_Sliding7d_CountGte5Days20Follows(t *testing.T) {
	env := setupTestEnv(t)
	eng, registry, bus := env.buildEngine(t)
	ctx := context.Background()

	const (
		biz    = "test"
		userID = int64(1005)
		ruleID = int64(1005)
	)

	rule := domain.Rule{
		Biz:       biz,
		ID:        ruleID,
		Name:      "社交达人",
		EventType: "follow",
		Condition: domain.ConditionConfig{Expression: "followed == true"},
		Window:    domain.WindowConfig{Type: "sliding", Size: "7d", BucketUnit: "day"},
		Aggregator: domain.AggregatorConfig{Type: "count"},
		Trigger:   domain.TriggerConfig{Type: "count_gte", Threshold: 20, Count: 5},
	}
	require.NoError(t, registry.Register(ctx, rule))

	// Map: day offset from (now - 6d) → follow count
	// 5 days with >=20 follows, 2 days with <20
	base := time.Now().UTC().Truncate(24 * time.Hour)
	followsPerDay := []int64{30, 25, 10, 21, 22, 23, 5} // days D-6 to D

	for dayOffset, count := range followsPerDay {
		ts := base.AddDate(0, 0, -(6 - dayOffset)).Unix()
		for i := int64(0); i < count; i++ {
			eng.Consume(ctx, domain.Event{
				EID: nextEID(), Type: "follow", Biz: biz, UserId: userID,
				Timestamp: ts,
				Payload:   map[string]interface{}{"followed": true},
			})
		}
	}

	waitTriggered(t, bus, ruleID, 1, 5*time.Second)
}

// ---- Rule 6: 活动期间累计消费金币>=100000 ----

func TestRule6_RangeWindow_ThresholdCoin(t *testing.T) {
	env := setupTestEnv(t)
	eng, registry, bus := env.buildEngine(t)
	ctx := context.Background()

	const (
		biz    = "test"
		userID = int64(1006)
		ruleID = int64(1006)
	)

	rule := domain.Rule{
		Biz:       biz,
		ID:        ruleID,
		Name:      "活动消费达人",
		EventType: "gift_send",
		Condition: domain.ConditionConfig{Expression: "coin > 0"},
		Window: domain.WindowConfig{
			Type:      "range",
			StartTime: "2026-05-01 00:00:00",
			EndTime:   "2026-05-31 23:59:59",
		},
		Aggregator: domain.AggregatorConfig{Type: "sum", Field: "coin"},
		Trigger:    domain.TriggerConfig{Type: "threshold", Threshold: 100000},
	}
	require.NoError(t, registry.Register(ctx, rule))

	// Use a timestamp inside the activity window
	activityTS := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC).Unix()

	eng.Consume(ctx, domain.Event{
		EID: nextEID(), Type: "gift_send", Biz: biz, UserId: userID,
		Timestamp: activityTS,
		Payload:   map[string]interface{}{"coin": int64(100000)},
	})

	waitTriggered(t, bus, ruleID, 1, 3*time.Second)

	st := store.NewStateStore(env.db, env.rdb)
	buckets, err := st.GetBuckets(ctx, biz, userID, ruleID, []string{"range"})
	require.NoError(t, err)
	require.Len(t, buckets, 1)
	require.GreaterOrEqual(t, buckets[0].Value, int64(100000))
}
