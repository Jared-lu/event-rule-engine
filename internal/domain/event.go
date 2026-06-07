package domain

type Event struct {
	EID       int64                  `json:"eid"` // 事件Id
	Type      string                 `json:"type"`
	Biz       string                 `json:"biz"`
	UserId    int64                  `json:"user_id"`
	Timestamp int64                  `json:"timestamp"`
	Payload   map[string]interface{} `json:"payload"`
}
