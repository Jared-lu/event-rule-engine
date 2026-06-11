# RuleRegistry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将规则的增删改查从 Engine 中剥离，引入 RuleDAO → RuleRepository → RuleRegistry 分层，Engine 通过 RuleRegistry 查找已编译规则，不再持有规则副本。

**Architecture:** RuleDAO 负责 GORM 操作；RuleRepository（domain 接口 + repository 实现）负责 domain 对象与 DAO 对象的转换；RuleRegistry（具体结构体）在启动时全量加载并编译所有规则到内存 map，动态 Register 时先持久化再编译追加，Remove 时先删库再从 map 移除；Engine 移除规则管理逻辑，每次 Consume 调用 registry.GetRule(eventType)。

**Tech Stack:** Go, GORM v2, MySQL, sync.RWMutex

---

## File Map

| 文件 | 动作 | 职责 |
|------|------|------|
| `internal/repository/rule_dao.go` | 新建 | RuleDAO：GORM 操作 rules 表 |
| `internal/repository/rule_repo.go` | 新建 | RuleRepository 实现：DAO 调用 + domain 转换 |
| `internal/domain/interfaces.go` | 修改 | 新增 RuleRepository 接口 |
| `internal/service/registry.go` | 新建 | RuleRegistry：内存 map + 启动加载 + 并发安全 |
| `internal/service/engine.go` | 修改 | 移除 rules map/RegisterRule/RemoveRule，注入 *RuleRegistry |
| `main.go` | 修改 | 组装 RuleRegistry 并注入 Engine |
| `internal/repository/rule_repo_test.go` | 新建 | RuleRepository 单元测试（mock DAO） |
| `internal/service/registry_test.go` | 新建 | RuleRegistry 单元测试（mock Repository） |

---

## Task 1: 新增 RuleRepository domain 接口

**Files:**
- Modify: `internal/domain/interfaces.go`

- [ ] **Step 1: 在 interfaces.go 末尾追加接口**

```go
// RuleRepository 规则的持久化接口
type RuleRepository interface {
	Save(ctx context.Context, rule Rule) error
	Delete(ctx context.Context, biz string, ruleID int64) error
	FindByEventType(ctx context.Context, eventType string) ([]Rule, error)
	FindAll(ctx context.Context) ([]Rule, error)
}
```

- [ ] **Step 2: 编译验证**

```bash
cd /home/lujianwei/go/src/event-rule-engine
go build ./...
```

Expected: 无错误输出。

- [ ] **Step 3: Commit**

```bash
git add internal/domain/interfaces.go
git commit -m "feat: add RuleRepository interface to domain"
```

---

## Task 2: 实现 RuleDAO

**Files:**
- Create: `internal/repository/rule_dao.go`

- [ ] **Step 1: 新建文件，定义 DAO 结构体和 GORM 模型**

```go
package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

// RuleModel GORM 映射表 rules
type RuleModel struct {
	ID          int64  `gorm:"primaryKey;autoIncrement"`
	Biz         string `gorm:"uniqueIndex:uk_biz_rule;not null;type:varchar(100)"`
	RuleID      int64  `gorm:"uniqueIndex:uk_biz_rule;not null;column:rule_id"`
	Name        string `gorm:"not null;type:varchar(200)"`
	Description string `gorm:"type:text"`
	EventType   string `gorm:"not null;index;type:varchar(100);column:event_type"`
	Config      string `gorm:"not null;type:text"` // domain.Rule JSON
	CreatedAt   int64  `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt   int64  `gorm:"column:updated_at;autoUpdateTime"`
}

func (RuleModel) TableName() string { return "rules" }

// RuleDAO GORM 操作 rules 表
type RuleDAO struct {
	db *gorm.DB
}

func NewRuleDAO(db *gorm.DB) *RuleDAO {
	return &RuleDAO{db: db}
}

// AutoMigrate 建表（开发/测试用）
func (d *RuleDAO) AutoMigrate() error {
	return d.db.AutoMigrate(&RuleModel{})
}

func (d *RuleDAO) Upsert(ctx context.Context, rule domain.Rule) error {
	data, err := json.Marshal(rule)
	if err != nil {
		return fmt.Errorf("rule_dao: marshal: %w", err)
	}
	now := time.Now().Unix()
	row := RuleModel{
		Biz:         rule.Biz,
		RuleID:      rule.ID,
		Name:        rule.Name,
		Description: rule.Description,
		EventType:   rule.EventType,
		Config:      string(data),
		UpdatedAt:   now,
	}
	res := d.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "biz"}, {Name: "rule_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"name", "description", "event_type", "config", "updated_at"}),
	}).Create(&row)
	return res.Error
}

func (d *RuleDAO) Delete(ctx context.Context, biz string, ruleID int64) error {
	res := d.db.WithContext(ctx).
		Where("biz = ? AND rule_id = ?", biz, ruleID).
		Delete(&RuleModel{})
	return res.Error
}

func (d *RuleDAO) FindByEventType(ctx context.Context, eventType string) ([]RuleModel, error) {
	var rows []RuleModel
	err := d.db.WithContext(ctx).
		Where("event_type = ?", eventType).
		Find(&rows).Error
	return rows, err
}

func (d *RuleDAO) FindAll(ctx context.Context) ([]RuleModel, error) {
	var rows []RuleModel
	err := d.db.WithContext(ctx).Find(&rows).Error
	return rows, err
}

// toRule 将 RuleModel 反序列化为 domain.Rule
func toRule(row RuleModel) (domain.Rule, error) {
	var rule domain.Rule
	if err := json.Unmarshal([]byte(row.Config), &rule); err != nil {
		return domain.Rule{}, fmt.Errorf("rule_dao: unmarshal rule %d: %w", row.RuleID, err)
	}
	return rule, nil
}

var ErrRuleNotFound = errors.New("rule_dao: rule not found")
```

- [ ] **Step 2: 编译验证**

```bash
go build ./internal/repository/...
```

Expected: 无错误输出。

- [ ] **Step 3: Commit**

```bash
git add internal/repository/rule_dao.go
git commit -m "feat: add RuleDAO with GORM mapping for rules table"
```

---

## Task 3: 实现 RuleRepository

**Files:**
- Create: `internal/repository/rule_repo.go`
- Create: `internal/repository/rule_repo_test.go`

- [ ] **Step 1: 写失败测试**

新建 `internal/repository/rule_repo_test.go`：

```go
package repository_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
	"github.com/Jared-lu/event-rule-engine/internal/repository"
)

func TestRuleRepository_SaveAndFindByEventType(t *testing.T) {
	dao := repository.NewMockRuleDAO()
	repo := repository.NewRuleRepository(dao)
	ctx := context.Background()

	rule := domain.Rule{
		Biz:       "test",
		ID:        1,
		Name:      "test rule",
		EventType: "gift_send",
		Condition: domain.ConditionConfig{Expression: "coin > 0"},
		Window:    domain.WindowConfig{Type: "global"},
		Aggregator: domain.AggregatorConfig{Type: "count"},
		Trigger:   domain.TriggerConfig{Type: "every", Step: 100},
	}

	require.NoError(t, repo.Save(ctx, rule))

	rules, err := repo.FindByEventType(ctx, "gift_send")
	require.NoError(t, err)
	require.Len(t, rules, 1)
	assert.Equal(t, rule.ID, rules[0].ID)
	assert.Equal(t, rule.EventType, rules[0].EventType)
}

func TestRuleRepository_Delete(t *testing.T) {
	dao := repository.NewMockRuleDAO()
	repo := repository.NewRuleRepository(dao)
	ctx := context.Background()

	rule := domain.Rule{Biz: "test", ID: 2, EventType: "recharge",
		Window: domain.WindowConfig{Type: "global"},
		Aggregator: domain.AggregatorConfig{Type: "count"},
		Trigger:   domain.TriggerConfig{Type: "every", Step: 1},
	}
	require.NoError(t, repo.Save(ctx, rule))
	require.NoError(t, repo.Delete(ctx, "test", 2))

	rules, err := repo.FindByEventType(ctx, "recharge")
	require.NoError(t, err)
	assert.Empty(t, rules)
}

func TestRuleRepository_FindAll(t *testing.T) {
	dao := repository.NewMockRuleDAO()
	repo := repository.NewRuleRepository(dao)
	ctx := context.Background()

	for i := int64(1); i <= 3; i++ {
		require.NoError(t, repo.Save(ctx, domain.Rule{
			Biz: "test", ID: i, EventType: "ev",
			Window:     domain.WindowConfig{Type: "global"},
			Aggregator: domain.AggregatorConfig{Type: "count"},
			Trigger:    domain.TriggerConfig{Type: "every", Step: 1},
		}))
	}
	rules, err := repo.FindAll(ctx)
	require.NoError(t, err)
	assert.Len(t, rules, 3)
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
go test ./internal/repository/... -v -run "TestRuleRepository"
```

Expected: FAIL（`repository.NewMockRuleDAO` 和 `repository.NewRuleRepository` 未定义）

- [ ] **Step 3: 实现 RuleRepository 和 MockRuleDAO**

新建 `internal/repository/rule_repo.go`：

```go
package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

// ruleDAOIface RuleDAO 的最小接口，方便测试替换
type ruleDAOIface interface {
	Upsert(ctx context.Context, rule domain.Rule) error
	Delete(ctx context.Context, biz string, ruleID int64) error
	FindByEventType(ctx context.Context, eventType string) ([]RuleModel, error)
	FindAll(ctx context.Context) ([]RuleModel, error)
}

// RuleRepositoryImpl 实现 domain.RuleRepository
type RuleRepositoryImpl struct {
	dao ruleDAOIface
}

func NewRuleRepository(dao ruleDAOIface) domain.RuleRepository {
	return &RuleRepositoryImpl{dao: dao}
}

func (r *RuleRepositoryImpl) Save(ctx context.Context, rule domain.Rule) error {
	return r.dao.Upsert(ctx, rule)
}

func (r *RuleRepositoryImpl) Delete(ctx context.Context, biz string, ruleID int64) error {
	return r.dao.Delete(ctx, biz, ruleID)
}

func (r *RuleRepositoryImpl) FindByEventType(ctx context.Context, eventType string) ([]domain.Rule, error) {
	rows, err := r.dao.FindByEventType(ctx, eventType)
	if err != nil {
		return nil, err
	}
	return rowsToRules(rows)
}

func (r *RuleRepositoryImpl) FindAll(ctx context.Context) ([]domain.Rule, error) {
	rows, err := r.dao.FindAll(ctx)
	if err != nil {
		return nil, err
	}
	return rowsToRules(rows)
}

func rowsToRules(rows []RuleModel) ([]domain.Rule, error) {
	rules := make([]domain.Rule, 0, len(rows))
	for _, row := range rows {
		rule, err := toRule(row)
		if err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}
	return rules, nil
}

// MockRuleDAO 内存实现，仅用于测试
type MockRuleDAO struct {
	mu   sync.RWMutex
	rows map[string]RuleModel // key: biz:ruleID
}

func NewMockRuleDAO() *MockRuleDAO {
	return &MockRuleDAO{rows: make(map[string]RuleModel)}
}

func (m *MockRuleDAO) Upsert(_ context.Context, rule domain.Rule) error {
	data, _ := json.Marshal(rule)
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%s:%d", rule.Biz, rule.ID)
	m.rows[key] = RuleModel{
		Biz:       rule.Biz,
		RuleID:    rule.ID,
		EventType: rule.EventType,
		Config:    string(data),
	}
	return nil
}

func (m *MockRuleDAO) Delete(_ context.Context, biz string, ruleID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.rows, fmt.Sprintf("%s:%d", biz, ruleID))
	return nil
}

func (m *MockRuleDAO) FindByEventType(_ context.Context, eventType string) ([]RuleModel, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []RuleModel
	for _, row := range m.rows {
		if row.EventType == eventType {
			result = append(result, row)
		}
	}
	return result, nil
}

func (m *MockRuleDAO) FindAll(_ context.Context) ([]RuleModel, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]RuleModel, 0, len(m.rows))
	for _, row := range m.rows {
		result = append(result, row)
	}
	return result, nil
}
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
go test ./internal/repository/... -v -run "TestRuleRepository"
```

Expected: PASS，3 个测试全部绿色。

- [ ] **Step 5: Commit**

```bash
git add internal/repository/rule_repo.go internal/repository/rule_repo_test.go
git commit -m "feat: add RuleRepository with mock DAO for testing"
```

---

## Task 4: 实现 RuleRegistry

**Files:**
- Create: `internal/service/registry.go`
- Create: `internal/service/registry_test.go`

- [ ] **Step 1: 写失败测试**

新建 `internal/service/registry_test.go`：

```go
package service_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
	"github.com/Jared-lu/event-rule-engine/internal/service"
)

// mockRuleRepo 内存实现 domain.RuleRepository，仅用于测试
type mockRuleRepo struct {
	rules []domain.Rule
}

func (m *mockRuleRepo) Save(_ context.Context, rule domain.Rule) error {
	for i, r := range m.rules {
		if r.Biz == rule.Biz && r.ID == rule.ID {
			m.rules[i] = rule
			return nil
		}
	}
	m.rules = append(m.rules, rule)
	return nil
}

func (m *mockRuleRepo) Delete(_ context.Context, biz string, ruleID int64) error {
	filtered := m.rules[:0]
	for _, r := range m.rules {
		if r.Biz != biz || r.ID != ruleID {
			filtered = append(filtered, r)
		}
	}
	m.rules = filtered
	return nil
}

func (m *mockRuleRepo) FindByEventType(_ context.Context, eventType string) ([]domain.Rule, error) {
	var result []domain.Rule
	for _, r := range m.rules {
		if r.EventType == eventType {
			result = append(result, r)
		}
	}
	return result, nil
}

func (m *mockRuleRepo) FindAll(_ context.Context) ([]domain.Rule, error) {
	cp := make([]domain.Rule, len(m.rules))
	copy(cp, m.rules)
	return cp, nil
}

func validRule(biz string, id int64, eventType string) domain.Rule {
	return domain.Rule{
		Biz:        biz,
		ID:         id,
		EventType:  eventType,
		Condition:  domain.ConditionConfig{Expression: ""},
		Window:     domain.WindowConfig{Type: "global"},
		Aggregator: domain.AggregatorConfig{Type: "count"},
		Trigger:    domain.TriggerConfig{Type: "every", Step: 1},
	}
}

func TestRuleRegistry_LoadOnStart(t *testing.T) {
	repo := &mockRuleRepo{rules: []domain.Rule{
		validRule("biz", 1, "ev_a"),
		validRule("biz", 2, "ev_b"),
	}}
	reg, err := service.NewRuleRegistry(context.Background(), repo)
	require.NoError(t, err)

	rules := reg.GetRule("ev_a")
	require.Len(t, rules, 1)
	assert.Equal(t, int64(1), rules[0].Rule.ID)

	rules = reg.GetRule("ev_b")
	require.Len(t, rules, 1)
	assert.Equal(t, int64(2), rules[0].Rule.ID)
}

func TestRuleRegistry_Register(t *testing.T) {
	repo := &mockRuleRepo{}
	reg, err := service.NewRuleRegistry(context.Background(), repo)
	require.NoError(t, err)

	rule := validRule("biz", 10, "ev_x")
	require.NoError(t, reg.Register(context.Background(), rule))

	rules := reg.GetRule("ev_x")
	require.Len(t, rules, 1)
	assert.Equal(t, int64(10), rules[0].Rule.ID)

	// 验证已持久化
	persisted, _ := repo.FindByEventType(context.Background(), "ev_x")
	require.Len(t, persisted, 1)
}

func TestRuleRegistry_Remove(t *testing.T) {
	repo := &mockRuleRepo{rules: []domain.Rule{
		validRule("biz", 5, "ev_y"),
	}}
	reg, err := service.NewRuleRegistry(context.Background(), repo)
	require.NoError(t, err)

	require.NoError(t, reg.Remove(context.Background(), "biz", 5, "ev_y"))

	assert.Empty(t, reg.GetRule("ev_y"))

	persisted, _ := repo.FindByEventType(context.Background(), "ev_y")
	assert.Empty(t, persisted)
}

func TestRuleRegistry_GetRule_UnknownEventType(t *testing.T) {
	repo := &mockRuleRepo{}
	reg, err := service.NewRuleRegistry(context.Background(), repo)
	require.NoError(t, err)

	rules := reg.GetRule("no_such_event")
	assert.Empty(t, rules)
}

func TestRuleRegistry_ConcurrentRegisterAndGet(t *testing.T) {
	repo := &mockRuleRepo{}
	reg, err := service.NewRuleRegistry(context.Background(), repo)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		for i := int64(0); i < 100; i++ {
			_ = reg.Register(context.Background(), validRule("biz", i, "ev_z"))
		}
		close(done)
	}()
	for i := 0; i < 100; i++ {
		_ = reg.GetRule("ev_z")
	}
	<-done
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
go test ./internal/service/... -v -run "TestRuleRegistry"
```

Expected: FAIL（`service.NewRuleRegistry` 未定义）

- [ ] **Step 3: 实现 RuleRegistry**

新建 `internal/service/registry.go`：

```go
package service

import (
	"context"
	"fmt"
	"sync"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

// RuleRegistry 管理规则的生命周期：持久化、编译、内存索引
// 并发安全：读写均通过 mu 保护
type RuleRegistry struct {
	mu       sync.RWMutex
	compiled map[string][]CompiledRule // eventType -> compiled rules
	repo     domain.RuleRepository
}

// NewRuleRegistry 创建 RuleRegistry 并全量加载、编译所有规则
func NewRuleRegistry(ctx context.Context, repo domain.RuleRepository) (*RuleRegistry, error) {
	r := &RuleRegistry{
		compiled: make(map[string][]CompiledRule),
		repo:     repo,
	}
	rules, err := repo.FindAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("registry: load rules: %w", err)
	}
	for _, rule := range rules {
		compiled, err := Compile(rule)
		if err != nil {
			return nil, fmt.Errorf("registry: compile rule %d: %w", rule.ID, err)
		}
		r.compiled[rule.EventType] = append(r.compiled[rule.EventType], compiled)
	}
	return r, nil
}

// Register 持久化规则，编译后追加到内存索引
func (r *RuleRegistry) Register(ctx context.Context, rule domain.Rule) error {
	compiled, err := Compile(rule)
	if err != nil {
		return fmt.Errorf("registry: compile rule %d: %w", rule.ID, err)
	}
	if err := r.repo.Save(ctx, rule); err != nil {
		return fmt.Errorf("registry: save rule %d: %w", rule.ID, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.compiled[rule.EventType] = append(r.compiled[rule.EventType], compiled)
	return nil
}

// Remove 从库和内存中删除规则
func (r *RuleRegistry) Remove(ctx context.Context, biz string, ruleID int64, eventType string) error {
	if err := r.repo.Delete(ctx, biz, ruleID); err != nil {
		return fmt.Errorf("registry: delete rule %d: %w", ruleID, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rules := r.compiled[eventType]
	filtered := rules[:0]
	for _, c := range rules {
		if c.Rule.Biz != biz || c.Rule.ID != ruleID {
			filtered = append(filtered, c)
		}
	}
	r.compiled[eventType] = filtered
	return nil
}

// GetRule 返回订阅指定 eventType 的已编译规则列表（返回副本，并发安全）
func (r *RuleRegistry) GetRule(eventType string) []CompiledRule {
	r.mu.RLock()
	defer r.mu.RUnlock()
	src := r.compiled[eventType]
	if len(src) == 0 {
		return nil
	}
	cp := make([]CompiledRule, len(src))
	copy(cp, src)
	return cp
}
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
go test ./internal/service/... -v -run "TestRuleRegistry"
```

Expected: PASS，5 个测试全部绿色。

- [ ] **Step 5: Commit**

```bash
git add internal/service/registry.go internal/service/registry_test.go
git commit -m "feat: add RuleRegistry with startup loading and concurrent-safe map"
```

---

## Task 5: 重构 Engine

**Files:**
- Modify: `internal/service/engine.go`

- [ ] **Step 1: 替换 Engine 结构体和构造函数**

将 `internal/service/engine.go` 中：
- 删除 `mu sync.RWMutex` 字段
- 删除 `rules map[string][]CompiledRule` 字段
- 新增 `registry *RuleRegistry` 字段
- 更新 `NewEngine` 签名

完整替换后的文件：

```go
package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

// Engine 负责消费 Event，按 EventType 通过 RuleRegistry 找出规则并并发执行
type Engine struct {
	registry    *RuleRegistry
	store       domain.StateStore
	eventBus    domain.EventBus
	idempotency Idempotency
}

// Idempotency 幂等检查接口，使用 Redis SET NX 实现
type Idempotency interface {
	CheckAndSet(ctx context.Context, eid int64) (alreadyProcessed bool, err error)
}

func NewEngine(registry *RuleRegistry, store domain.StateStore, eventBus domain.EventBus, idempotency Idempotency) *Engine {
	return &Engine{
		registry:    registry,
		store:       store,
		eventBus:    eventBus,
		idempotency: idempotency,
	}
}

// Consume 消费一个 Event
func (e *Engine) Consume(ctx context.Context, event domain.Event) {
	// 幂等检查
	already, err := e.idempotency.CheckAndSet(ctx, event.EID)
	if err != nil || already {
		return
	}

	rules := e.registry.GetRule(event.Type)

	for _, rule := range rules {
		go e.match(ctx, event, rule)
	}
}

func (e *Engine) match(ctx context.Context, event domain.Event, rule CompiledRule) {
	// 1. Condition 过滤
	matched, err := rule.Condition.Match(event.Payload)
	if err != nil || !matched {
		return
	}

	// 2. 确定 bucket key
	key := rule.Window.BucketKey(event.Timestamp)
	if key == "" {
		// range window 且事件不在范围内
		return
	}

	// 3. 提取增量值
	delta, err := rule.Aggregator.Extract(event.Payload)
	if err != nil || delta == 0 {
		return
	}

	// 4. 原子更新 bucket
	r := rule.Rule
	if err := e.store.IncrBucket(ctx, r.Biz, event.UserId, r.ID, key, delta); err != nil {
		return
	}

	// 5. 取触发判断所需的 keys
	now := time.Now().Unix()
	activeKeys := rule.Window.ActiveKeys(now)

	// 6. 读取 buckets
	buckets, err := e.store.GetBuckets(ctx, r.Biz, event.UserId, r.ID, activeKeys)
	if err != nil {
		return
	}

	// 7. 读取当前进度
	progress, err := e.store.GetProgress(ctx, r.Biz, event.UserId, r.ID)
	if err != nil {
		return
	}

	// 8. 触发判断
	triggered, nextThreshold := rule.Trigger.Check(buckets, progress)
	if !triggered {
		return
	}

	// 9. 更新进度（乐观锁，失败由 Store 实现重试）
	total := sumBuckets(buckets)
	updated := domain.RuleProgress{
		Biz:             r.Biz,
		UserID:          event.UserId,
		RuleID:          r.ID,
		Buckets:         buckets,
		CurrentLevel:    progress.CurrentLevel + 1,
		NextThreshold:   nextThreshold,
		LastTriggeredAt: now,
		Version:         progress.Version,
	}
	if err := e.store.SaveProgress(ctx, updated); err != nil {
		return
	}

	// 10. 发布平台事件
	_ = e.eventBus.Publish(ctx, domain.RuleEvent{
		Biz:          r.Biz,
		UserID:       event.UserId,
		RuleID:       r.ID,
		CurrentValue: total,
		Threshold:    nextThreshold - (nextThreshold - progress.NextThreshold),
	})
}

func sumBuckets(buckets []domain.Bucket) int64 {
	var total int64
	for _, b := range buckets {
		total += b.Value
	}
	return total
}

// compile-time check: sync is still imported
var _ = sync.RWMutex{}
var _ = fmt.Sprintf
```

> 注意：最后两行 `var _` 只是为了确保包引用不报错，如果不需要可删掉。删掉 `sync` 和 `fmt` 的 import，只保留实际用到的。

实际清理后的 import 应为：

```go
import (
	"context"
	"time"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
)
```

- [ ] **Step 2: 编译验证**

```bash
go build ./internal/service/...
```

Expected: 无错误输出。

- [ ] **Step 3: Commit**

```bash
git add internal/service/engine.go
git commit -m "refactor: engine delegates rule lookup to RuleRegistry"
```

---

## Task 6: 更新集成测试和 main.go

**Files:**
- Modify: `internal/integration/engine_integration_test.go`
- Modify: `main.go`

- [ ] **Step 1: 更新集成测试的 buildEngine 方法**

在 `internal/integration/engine_integration_test.go` 中，找到 `buildEngine` 函数（第 113 行），替换为：

```go
func (e *testEnv) buildEngine(t *testing.T) (*service.Engine, *mockEventBus) {
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
```

并更新每个测试函数中的调用，将：
```go
eng, bus := env.buildEngine(t)
```
替换为：
```go
eng, registry, bus := env.buildEngine(t)
```

并将每个测试中的 `eng.RegisterRule(rule)` 替换为：
```go
require.NoError(t, registry.Register(ctx, rule))
```

在文件顶部 import 中新增：
```go
"github.com/Jared-lu/event-rule-engine/internal/repository"
```

- [ ] **Step 2: 更新 main.go**

在 `main.go` 中，找到 `// --- Engine ---` 注释块，替换为：

```go
// --- Rule Repository & Registry ---
ruleDAO := repository.NewRuleDAO(db)
if err := ruleDAO.AutoMigrate(); err != nil {
	log.Fatalf("rule migrate: %v", err)
}
ruleRepo := repository.NewRuleRepository(ruleDAO)
registry, err := service.NewRuleRegistry(context.Background(), ruleRepo)
if err != nil {
	log.Fatalf("registry: %v", err)
}

// --- Engine ---
engine := service.NewEngine(registry, stateStore, eventBus, idempotency)
```

在 import 中新增：
```go
"github.com/Jared-lu/event-rule-engine/internal/repository"
```

- [ ] **Step 3: 编译整个项目**

```bash
go build ./...
```

Expected: 无错误输出。

- [ ] **Step 4: 运行单元测试**

```bash
go test ./internal/service/... ./internal/repository/... -v
```

Expected: 所有单元测试 PASS。

- [ ] **Step 5: Commit**

```bash
git add internal/integration/engine_integration_test.go main.go
git commit -m "feat: wire RuleRegistry into Engine and update integration test"
```

---

## Task 7: 更新 store/idempotency.go 中的循环依赖

**Files:**
- Modify: `internal/store/idempotency.go`

> 当前 `idempotency.go` import 了 `internal/service`（为了 `service.Idempotency` 接口）。重构后 Engine 不再导出该接口相关类型，需确认是否有循环依赖。

- [ ] **Step 1: 检查循环依赖**

```bash
go build ./...
```

如果有 `import cycle` 错误，将 `Idempotency` 接口从 `service` 包移到 `domain` 包：

在 `internal/domain/interfaces.go` 末尾追加：
```go
// Idempotency 幂等检查，防止同一事件重复处理
type Idempotency interface {
	CheckAndSet(ctx context.Context, eid int64) (alreadyProcessed bool, err error)
}
```

然后将 `internal/store/idempotency.go` 中的 import 从 `service` 改为 `domain`，`NewRedisIdempotency` 返回类型改为 `domain.Idempotency`，并更新 `internal/service/engine.go` 中 `Idempotency` 类型改用 `domain.Idempotency`。

- [ ] **Step 2: 编译验证**

```bash
go build ./...
```

Expected: 无错误。

- [ ] **Step 3: Commit（如有改动）**

```bash
git add internal/domain/interfaces.go internal/store/idempotency.go internal/service/engine.go
git commit -m "fix: move Idempotency interface to domain to break import cycle"
```
