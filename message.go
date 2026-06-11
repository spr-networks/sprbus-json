package sprbus

// Message replaces the protobuf pb.String type.
// Kept compatible so callers that check .Topic / .Value still work.
type Message struct {
	Topic string `json:"topic"`
	Value string `json:"value"`
}

func (m *Message) GetTopic() string { return m.Topic }
func (m *Message) GetValue() string { return m.Value }
