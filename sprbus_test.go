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
