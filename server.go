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
	quit   chan struct{}
	once   sync.Once
	filter func(string) bool
	local  bool
}

func (s *subscriber) stop() {
	s.once.Do(func() { close(s.quit) })
}

// a subscriber more than bufferSize behind gets evictTimeout of grace while
// the bus blocks; past that a socket subscriber is disconnected (it can
// reconnect), in-process subscribers are never dropped
const bufferSize = 4096
const evictTimeout = 5 * time.Second

func newPublisher() *publisher {
	return &publisher{subs: make(map[*subscriber]struct{})}
}

func (p *publisher) publish(ev event) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for sub := range p.subs {
		if sub.filter != nil && !sub.filter(ev.topic) {
			continue
		}
		select {
		case sub.ch <- ev:
			continue
		default:
		}
		// buffer full: block rather than drop
		if sub.local {
			select {
			case sub.ch <- ev:
			case <-sub.quit:
			}
			continue
		}
		t := time.NewTimer(evictTimeout)
		select {
		case sub.ch <- ev:
			t.Stop()
		case <-sub.quit:
			t.Stop()
		case <-t.C:
			log.Printf("[sprbus] disconnecting stalled subscriber")
			sub.stop()
		}
	}
}

func (p *publisher) subscribe(filter func(string) bool, local bool) *subscriber {
	sub := &subscriber{
		ch:     make(chan event, bufferSize),
		quit:   make(chan struct{}),
		filter: filter,
		local:  local,
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
	sub.stop()
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
	}, true)

	go func() {
		for {
			select {
			case ev := <-sub.ch:
				callback(ev.topic, ev.raw)
			case <-sub.quit:
				return
			}
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
			}, false)
			defer s.pub.unsubscribe(sub)

			// Send events until the connection closes; the original line
			// is passed through, no re-encoding. Queued events coalesce
			// into a single flush to keep the drain faster than the bus.
			w := bufio.NewWriter(conn)
			for {
				var ev event
				select {
				case ev = <-sub.ch:
				case <-sub.quit:
					return
				}
				if _, err := w.Write(ev.raw); err != nil {
					return
				}
				if err := w.WriteByte('\n'); err != nil {
					return
				}
			coalesce:
				for {
					select {
					case more := <-sub.ch:
						if _, err := w.Write(more.raw); err != nil {
							return
						}
						if err := w.WriteByte('\n'); err != nil {
							return
						}
					case <-sub.quit:
						w.Flush()
						return
					default:
						break coalesce
					}
				}
				if err := w.Flush(); err != nil {
					return
				}
			}
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
