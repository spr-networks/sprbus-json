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

// publisher is a simple in-process pub/sub (replaces github.com/moby/pubsub)
type publisher struct {
	mu   sync.RWMutex
	subs map[*subscriber]struct{}
}

type subscriber struct {
	ch     chan Message
	filter func(Message) bool
}

func newPublisher() *publisher {
	return &publisher{subs: make(map[*subscriber]struct{})}
}

func (p *publisher) publish(msg Message) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for sub := range p.subs {
		if sub.filter == nil || sub.filter(msg) {
			select {
			case sub.ch <- msg:
			default:
				// drop if subscriber is slow
			}
		}
	}
}

func (p *publisher) subscribe(filter func(Message) bool) *subscriber {
	sub := &subscriber{
		ch:     make(chan Message, 64),
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

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 256*1024), 256*1024)

	for scanner.Scan() {
		var msg Message
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			continue
		}

		if strings.HasPrefix(msg.Topic, "subscribe:") {
			// Subscribe mode: filter by topic prefix, stream events back
			prefix := strings.TrimPrefix(msg.Topic, "subscribe:")
			sub := s.pub.subscribe(func(m Message) bool {
				return strings.HasPrefix(m.Topic, prefix)
			})
			defer s.pub.unsubscribe(sub)

			// Send events until connection closes
			enc := json.NewEncoder(conn)
			for ev := range sub.ch {
				if err := enc.Encode(ev); err != nil {
					return
				}
			}
			return
		}

		// Publish mode
		s.pub.publish(msg)
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
