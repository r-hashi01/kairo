package n8n

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// LifecycleEvent is one of engine v2's six lifecycle events.
type LifecycleEvent struct {
	Type        string          `json:"type"` // execution:started|completed|failed, step:started|completed|failed
	ExecutionID string          `json:"executionId"`
	WorkflowID  string          `json:"workflowId,omitempty"`
	StepID      string          `json:"stepId,omitempty"`
	NodeID      string          `json:"nodeId,omitempty"`
	NodeName    string          `json:"nodeName,omitempty"`
	Iteration   *int            `json:"iteration,omitempty"`
	At          string          `json:"at"`
	Mode        string          `json:"mode,omitempty"`
	HostMode    string          `json:"hostMode,omitempty"`
	Outputs     json.RawMessage `json:"outputs,omitempty"`
}

// Events sends lifecycle events to the control plane as engine v2 does:
// batched every 50ms (or at 500 events), one batch in flight, in order, at
// most once: a batch that fails is dropped (the control plane reads the
// execution again to be sure). Beyond 10000 waiting events, new ones are
// dropped.
type Events struct {
	URL    string // the control plane's base URL
	Secret []byte
	Client *http.Client

	mu      sync.Mutex
	pending []LifecycleEvent
	wake    chan struct{}
}

const (
	eventsPerBatch = 500
	eventsWaiting  = 20 * eventsPerBatch
	eventsEvery    = 50 * time.Millisecond
	eventsTimeout  = 10 * time.Second
)

// Send queues events.
func (s *Events) Send(evs ...LifecycleEvent) {
	if s == nil || s.URL == "" {
		return
	}
	s.mu.Lock()
	dropped := 0
	for _, ev := range evs {
		if len(s.pending) >= eventsWaiting {
			dropped++
			continue
		}
		s.pending = append(s.pending, ev)
	}
	full := len(s.pending) >= eventsPerBatch
	s.mu.Unlock()
	if dropped > 0 {
		log.Printf("kairo-n8n: %d lifecycle events dropped (too many waiting)", dropped)
	}
	if full {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

// Run sends batches until ctx ends, then what is left (within 10s).
func (s *Events) Run(ctx context.Context) {
	t := time.NewTicker(eventsEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			end, cancel := context.WithTimeout(context.Background(), eventsTimeout)
			for s.flush(end) {
			}
			cancel()
			return
		case <-t.C:
		case <-s.wake:
		}
		for s.flush(ctx) {
		}
	}
}

// flush sends one batch; true while more are waiting.
func (s *Events) flush(ctx context.Context) bool {
	s.mu.Lock()
	n := min(len(s.pending), eventsPerBatch)
	batch := s.pending[:n:n]
	s.pending = s.pending[n:]
	more := len(s.pending) > 0
	s.mu.Unlock()
	if n == 0 {
		return false
	}
	if err := s.post(ctx, batch); err != nil {
		log.Printf("kairo-n8n: %d lifecycle events not delivered: %v", n, err)
	}
	return more && ctx.Err() == nil
}

func (s *Events) post(ctx context.Context, batch []LifecycleEvent) error {
	body, err := json.Marshal(map[string]any{"events": batch})
	if err != nil {
		return err
	}
	tok, err := ActionToken(s.Secret, ScopeEvents, time.Now())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, eventsTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", s.URL+"/internal/status-callback", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+tok)
	c := s.Client
	if c == nil {
		c = http.DefaultClient
	}
	res, err := c.Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	if res.StatusCode/100 != 2 {
		return fmt.Errorf("status %d", res.StatusCode)
	}
	return nil
}

// Responses publishes engine v2's execution responses (the "ended" frame
// that tells a waiting webhook how the execution ended) on Redis, on the
// channel the control plane listens to: <prefix>:engine-v2-responses:<id>.
// It speaks the little of RESP it needs (AUTH, PUBLISH) over one
// connection, reconnecting on errors, so that kairo-n8n needs no Redis
// library.
type Responses struct {
	Addr     string // host:port
	Password string
	Prefix   string // n8n's redis prefix (default "n8n")

	mu   sync.Mutex
	conn net.Conn
	rd   *bufio.Reader
}

// maxFrame is the largest response the control plane accepts.
const maxFrame = 5 << 20

// Publish sends one response frame. A frame that is too large is replaced
// by an "undeliverable" one, as engine v2 does.
func (r *Responses) Publish(executionID string, frame any) error {
	if r == nil || r.Addr == "" {
		return nil
	}
	b, err := json.Marshal(frame)
	if err == nil && len(b) > maxFrame {
		err = errors.New("response frame larger than 5MB")
	}
	if err != nil {
		b, _ = json.Marshal(map[string]any{"type": "undeliverable", "executionId": executionID,
			"error": map[string]string{"code": "frame_too_large", "message": err.Error()}})
	}
	prefix := r.Prefix
	if prefix == "" {
		prefix = "n8n"
	}
	channel := prefix + ":engine-v2-responses:" + executionID
	r.mu.Lock()
	defer r.mu.Unlock()
	for attempt := 0; attempt < 2; attempt++ {
		if err = r.dial(); err == nil {
			if err = r.command("PUBLISH", channel, string(b)); err == nil {
				return nil
			}
		}
		r.close()
	}
	return err
}

func (r *Responses) dial() error {
	if r.conn != nil {
		return nil
	}
	c, err := net.DialTimeout("tcp", r.Addr, 5*time.Second)
	if err != nil {
		return err
	}
	r.conn, r.rd = c, bufio.NewReader(c)
	if r.Password != "" {
		if err := r.command("AUTH", r.Password); err != nil {
			r.close()
			return fmt.Errorf("redis AUTH: %w", err)
		}
	}
	return nil
}

func (r *Responses) close() {
	if r.conn != nil {
		r.conn.Close()
		r.conn, r.rd = nil, nil
	}
}

// command sends a RESP array of bulk strings and reads one reply.
func (r *Responses) command(args ...string) error {
	r.conn.SetDeadline(time.Now().Add(5 * time.Second))
	var b bytes.Buffer
	b.WriteString("*" + strconv.Itoa(len(args)) + "\r\n")
	for _, a := range args {
		b.WriteString("$" + strconv.Itoa(len(a)) + "\r\n" + a + "\r\n")
	}
	if _, err := r.conn.Write(b.Bytes()); err != nil {
		return err
	}
	line, err := r.rd.ReadString('\n')
	if err != nil {
		return err
	}
	if len(line) > 0 && line[0] == '-' {
		return errors.New("redis: " + line[1:len(line)-2])
	}
	return nil
}
