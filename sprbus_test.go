package sprbus

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPubSub(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "test.sock")
	os.Setenv("TEST_PREFIX", "")
	ServerEventSock = sock

	srv, err := NewServer(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	// subscriber
	received := make(chan Message, 10)
	go func() {
		HandleEvent("test:", func(topic, value string) {
			received <- Message{Topic: topic, Value: value}
		})
	}()

	time.Sleep(50 * time.Millisecond)

	// publish
	_, err = Publish("test:hello", map[string]string{"msg": "world"})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-received:
		if msg.Topic != "test:hello" {
			t.Errorf("expected topic test:hello, got %s", msg.Topic)
		}
		if msg.Value == "" {
			t.Error("expected non-empty value")
		}
		t.Logf("OK: %s = %s", msg.Topic, msg.Value)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestNilClient(t *testing.T) {
	// Verify no panic when server is unreachable
	ServerEventSock = "/tmp/nonexistent-sprbus.sock"

	_, err := Publish("test:topic", map[string]string{"key": "val"})
	if err == nil {
		t.Fatal("expected error for unreachable server")
	}

	_, err = PublishString("test:topic", "hello")
	if err == nil {
		t.Fatal("expected error for unreachable server")
	}

	// NewLog should not panic even with no server
	log := NewLog("test")
	log.Info("this should not panic")
}

func TestNilClientMethods(t *testing.T) {
	var c *Client
	c.Close() // should not panic

	_, err := c.Publish("topic", "value")
	if err == nil {
		t.Fatal("expected error from nil client Publish")
	}

	_, err = c.SubscribeTopic("topic")
	if err == nil {
		t.Fatal("expected error from nil client SubscribeTopic")
	}
}

func TestPublishString(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "test.sock")
	ServerEventSock = sock

	srv, err := NewServer(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	received := make(chan Message, 10)
	go func() {
		HandleEvent("log:", func(topic, value string) {
			received <- Message{Topic: topic, Value: value}
		})
	}()

	time.Sleep(50 * time.Millisecond)

	_, err = PublishString("log:info", `{"level":"info","msg":"test"}`)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-received:
		if msg.Topic != "log:info" {
			t.Errorf("expected topic log:info, got %s", msg.Topic)
		}
		t.Logf("OK: %s = %s", msg.Topic, msg.Value)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestRawPassthrough(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "raw.sock")
	ServerEventSock = sock

	srv, err := NewServer(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	client, err := NewClient(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	stream, err := client.SubscribeTopic("raw:")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	// value with escapes and non-ASCII to prove byte fidelity
	value := `{"name":"Zürich \"quoted\"","n":42}`
	if _, err := PublishString("raw:test", value); err != nil {
		t.Fatal(err)
	}

	msg, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if msg.Topic != "raw:test" || msg.Value != value {
		t.Fatalf("passthrough mismatch: %q %q", msg.Topic, msg.Value)
	}
}

func TestServerHandleEvent(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "local.sock")
	ServerEventSock = sock

	srv, err := NewServer(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	got := make(chan Message, 10)
	cancel := srv.HandleEvent("local:", func(topic, value string) {
		got <- Message{Topic: topic, Value: value}
	})
	defer cancel()

	rawTopics := make(chan string, 10)
	cancelRaw := srv.HandleEventRaw("", func(topic string, raw []byte) {
		rawTopics <- topic
	})
	defer cancelRaw()

	if _, err := PublishString("local:hit", `{"a":1}`); err != nil {
		t.Fatal(err)
	}
	if _, err := PublishString("other:miss", `{"b":2}`); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-got:
		if msg.Topic != "local:hit" || msg.Value != `{"a":1}` {
			t.Fatalf("unexpected: %+v", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for local delivery")
	}

	// prefix-filtered subscriber must not see the other topic
	select {
	case msg := <-got:
		t.Fatalf("prefix filter leaked: %+v", msg)
	case <-time.After(100 * time.Millisecond):
	}

	// unfiltered raw subscriber sees both
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case topic := <-rawTopics:
			seen[topic] = true
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for raw delivery")
		}
	}
	if !seen["local:hit"] || !seen["other:miss"] {
		t.Fatalf("raw subscriber missed topics: %v", seen)
	}
}
