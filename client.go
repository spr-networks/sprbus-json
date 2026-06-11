package sprbus

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

var ServerEventSock = os.Getenv("TEST_PREFIX") + "/state/api/eventbus.sock"

// Client connects to a sprbus server over a unix socket.
type Client struct {
	path string
	conn net.Conn
}

// NewClient connects to the sprbus server at the given unix socket path.
// Retries briefly to handle the common case where the server goroutine
// hasn't created the socket yet.
func NewClient(socketPath string) (*Client, error) {
	if socketPath == "" {
		socketPath = ServerEventSock
	}

	var conn net.Conn
	var err error
	for i := 0; i < 20; i++ {
		conn, err = net.Dial("unix", socketPath)
		if err == nil {
			return &Client{path: socketPath, conn: conn}, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil, err
}

func (c *Client) Close() {
	if c != nil && c.conn != nil {
		c.conn.Close()
	}
}

// Publish sends a message to the bus. Returns a Message for API compatibility.
func (c *Client) Publish(topic string, value string) (*Message, error) {
	if c == nil || c.conn == nil {
		return nil, fmt.Errorf("sprbus: not connected")
	}
	msg := Message{Topic: topic, Value: value}
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	data = append(data, '\n')
	_, err = c.conn.Write(data)
	if err != nil {
		return nil, err
	}
	return &Message{}, nil
}

// SubscribeTopic subscribes to events matching a topic prefix.
// Returns a channel-based stream that mimics the gRPC stream interface.
func (c *Client) SubscribeTopic(topic string) (*Stream, error) {
	if c == nil || c.conn == nil {
		return nil, fmt.Errorf("sprbus: not connected")
	}
	// Send subscribe request
	msg := Message{Topic: "subscribe:" + topic}
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	data = append(data, '\n')
	_, err = c.conn.Write(data)
	if err != nil {
		return nil, err
	}

	return &Stream{scanner: bufio.NewScanner(c.conn)}, nil
}

// Subscribe subscribes to all events (empty topic prefix).
func (c *Client) Subscribe(topic string) (*Stream, error) {
	return c.SubscribeTopic("")
}

// Stream mimics the gRPC streaming interface.
type Stream struct {
	scanner *bufio.Scanner
}

func (s *Stream) Recv() (*Message, error) {
	if !s.scanner.Scan() {
		if err := s.scanner.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	var msg Message
	if err := json.Unmarshal(s.scanner.Bytes(), &msg); err != nil {
		return nil, fmt.Errorf("invalid message: %w", err)
	}
	return &msg, nil
}

// --- Package-level convenience functions (same signatures as original sprbus) ---

// PublishString publishes a string value to the default event bus.
func PublishString(topic string, value string) (*Message, error) {
	client, err := NewClient(ServerEventSock)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	return client.Publish(topic, value)
}

// Publish publishes a JSON-serializable value to the default event bus.
func Publish(topic string, value interface{}) (*Message, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return PublishString(topic, string(data))
}

// HandleEvent subscribes to a topic and calls the callback for each event.
// Blocks until the connection is closed or an error occurs.
func HandleEvent(topic string, callback func(string, string)) error {
	client, err := NewClient(ServerEventSock)
	if err != nil {
		return err
	}
	defer client.Close()

	stream, err := client.SubscribeTopic(topic)
	if err != nil {
		return err
	}

	for {
		reply, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil
		}
		callback(reply.GetTopic(), reply.GetValue())
	}

	return nil
}
