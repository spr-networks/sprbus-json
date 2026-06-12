package sprbus

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type Server struct {
	path     string
	listener net.Listener
	pub      *publisher
}

type event struct {
	topic string
	raw   []byte
}

type publisher struct {
	mu   sync.RWMutex
	subs map[*subscriber]struct{}
}

type subscriber struct {
	ch     chan event
	filter func(string) bool
}

func newPublisher() *publisher {
	return &publisher{subs: make(map[*subscriber]struct{})}
}

func (p *publisher) publish(ev event) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for sub := range p.subs {
		if sub.filter == nil || sub.filter(ev.topic) {
			select {
			case sub.ch <- ev:
			default:
				// drop if subscriber is slow
			}
		}
	}
}

func (p *publisher) subscribe(filter func(string) bool) *subscriber {
	sub := &subscriber{
		ch:     make(chan event, 64),
		filter: filter,
	}
	p.mu.Lock()
	p.subs[sub] = struct{}{}
	p.mu.Unlock()
	return sub
}

func (p *publisher) unsubscribe(sub *subscriber) {
	p.mu.Lock()
	delete(p.subs, sub)
	p.mu.Unlock()
	close(sub.ch)
}

// NewServer starts a pub/sub server on the given unix socket.
// Protocol: newline-delimited JSON messages.
//
//	Client sends: {"topic":"...","value":"..."}  → publish
//	Client sends: {"topic":"subscribe:..."}      → subscribe to topic prefix
//	Server sends: {"topic":"...","value":"..."}  → event to subscribers
func NewServer(socketPath string) (*Server, error) {
	if socketPath == "" {
		socketPath = ServerEventSock
	}

	os.Remove(socketPath)
	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, err
	}

	s := &Server{
		path:     socketPath,
		listener: lis,
		pub:      newPublisher(),
	}

	go s.acceptLoop()

	// small sleep to let the socket start accepting
	time.Sleep(10 * time.Millisecond)

	return s, nil
}

// HandleEvent subscribes in-process: events published on the bus are
// delivered to the callback without a socket round trip. The returned
// function cancels the subscription.
func (s *Server) HandleEvent(prefix string, callback func(topic string, value string)) func() {
	return s.HandleEventRaw(prefix, func(topic string, raw []byte) {
		var msg Message
		if err := json.Unmarshal(raw, &msg); err != nil {
			return
		}
		callback(msg.Topic, msg.Value)
	})
}

// HandleEventRaw subscribes in-process and delivers the original wire line,
// so the callback only pays for decoding the value if it uses it.
func (s *Server) HandleEventRaw(prefix string, callback func(topic string, raw []byte)) func() {
	sub := s.pub.subscribe(func(topic string) bool {
		return strings.HasPrefix(topic, prefix)
	})

	go func() {
		for ev := range sub.ch {
			callback(ev.topic, ev.raw)
		}
	}()

	return func() { s.pub.unsubscribe(sub) }
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

type envelope struct {
	Topic string `json:"topic"`
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 256*1024), 256*1024)

	for scanner.Scan() {
		var env envelope
		if err := json.Unmarshal(scanner.Bytes(), &env); err != nil {
			continue
		}

		if strings.HasPrefix(env.Topic, "subscribe:") {
			// Subscribe mode: filter by topic prefix, stream events back
			prefix := strings.TrimPrefix(env.Topic, "subscribe:")
			sub := s.pub.subscribe(func(topic string) bool {
				return strings.HasPrefix(topic, prefix)
			})
			defer s.pub.unsubscribe(sub)

			// Send events until the connection closes; the original line
			// is passed through, no re-encoding
			w := bufio.NewWriter(conn)
			for ev := range sub.ch {
				if _, err := w.Write(ev.raw); err != nil {
					return
				}
				if err := w.WriteByte('\n'); err != nil {
					return
				}
				if err := w.Flush(); err != nil {
					return
				}
			}
			return
		}

		// Publish mode: the scanner reuses its buffer, so the line is
		// copied once here and shared by all subscribers
		raw := make([]byte, len(scanner.Bytes()))
		copy(raw, scanner.Bytes())
		s.pub.publish(event{topic: env.Topic, raw: raw})
	}
}

func (s *Server) Close() {
	s.listener.Close()
}

// Serve blocks (like the original grpc server.Serve).
// Call NewServer then Serve if you want blocking behavior.
func (s *Server) Serve() error {
	log.Printf("[sprbus] serving on %s", s.path)
	// acceptLoop is already running in a goroutine from NewServer,
	// block forever
	select {}
}
