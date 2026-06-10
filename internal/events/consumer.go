package events

import (
	"encoding/json"

	"github.com/IBM/sarama"
	"github.com/Jared-lu/event-rule-engine/internal/domain"
	"github.com/Jared-lu/event-rule-engine/internal/service"
)

type ConsumerHandler struct {
	engine *service.Engine
}

func (c *ConsumerHandler) SetEngine(engine *service.Engine) {
	c.engine = engine
}

func (c *ConsumerHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *ConsumerHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			var event domain.Event
			err := json.Unmarshal(message.Value, &event)
			if err != nil {
				continue
			}

			// 开始消费
			// 交给规则规则引擎，去查找和匹配规则
			c.engine.Consume(session.Context(), event)

		case <-session.Context().Done():
			return nil
		}
	}
}
