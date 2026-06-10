package events

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

// KafkaEventBus 使用 Kafka 发布 RuleEvent
type KafkaEventBus struct {
	producer sarama.SyncProducer
	topic    string
}

func NewKafkaEventBus(producer sarama.SyncProducer, topic string) domain.EventBus {
	return &KafkaEventBus{producer: producer, topic: topic}
}

func (k *KafkaEventBus) Publish(_ context.Context, event domain.RuleEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("eventbus: marshal: %w", err)
	}
	msg := &sarama.ProducerMessage{
		Topic: k.topic,
		Key:   sarama.StringEncoder(fmt.Sprintf("%s:%d:%d", event.Biz, event.UserID, event.RuleID)),
		Value: sarama.ByteEncoder(data),
	}
	_, _, err = k.producer.SendMessage(msg)
	return err
}
