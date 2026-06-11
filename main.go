package main

import (
	"context"
	"log"
	"os"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/Jared-lu/event-rule-engine/internal/events"
	"github.com/Jared-lu/event-rule-engine/internal/pkg"
	"github.com/Jared-lu/event-rule-engine/internal/repository"
	"github.com/Jared-lu/event-rule-engine/internal/service"
	"github.com/Jared-lu/event-rule-engine/internal/web"
)

func main() {
	// --- MySQL ---
	dsn := getenv("MYSQL_DSN", "root:root@tcp(localhost:3306)/rule_engine?charset=utf8mb4&parseTime=True&loc=Local")
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("mysql: %v", err)
	}
	if err := repository.AutoMigrateState(db); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	// --- Redis ---
	rdb := redis.NewClient(&redis.Options{
		Addr:     getenv("REDIS_ADDR", "localhost:6379"),
		Password: getenv("REDIS_PASSWORD", ""),
	})

	// --- State Store & Idempotency ---
	stateStore := repository.NewStateStore(db, rdb)
	idempotency := pkg.NewRedisIdempotency(rdb)

	// --- Kafka Producer (EventBus) ---
	kafkaBrokers := []string{getenv("KAFKA_BROKER", "localhost:9092")}
	producerCfg := sarama.NewConfig()
	producerCfg.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(kafkaBrokers, producerCfg)
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}
	defer producer.Close()

	eventBus := events.NewKafkaEventBus(producer, getenv("RULE_EVENT_TOPIC", "rule-events"))

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

	// --- Kafka Consumer ---
	consumerGroup := getenv("KAFKA_CONSUMER_GROUP", "rule-engine")
	consumerCfg := sarama.NewConfig()
	group, err := sarama.NewConsumerGroup(kafkaBrokers, consumerGroup, consumerCfg)
	if err != nil {
		log.Fatalf("kafka consumer group: %v", err)
	}
	defer group.Close()

	bizEventTopic := getenv("BIZ_EVENT_TOPIC", "biz-events")
	handler := &events.ConsumerHandler{}
	handler.SetEngine(engine)

	go func() {
		for {
			if err := group.Consume(nil, []string{bizEventTopic}, handler); err != nil {
				log.Printf("consumer error: %v", err)
			}
		}
	}()

	// --- HTTP Server ---
	r := gin.Default()
	web.NewProgressHandler(stateStore).RegisterRoutes(r)

	addr := getenv("HTTP_ADDR", ":8080")
	log.Printf("HTTP server listening on %s", addr)
	if err := r.Run(addr); err != nil {
		log.Fatalf("http: %v", err)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
