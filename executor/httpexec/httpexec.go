// Package httpexec is the built-in HTTP executor, meant for LLM calls. It
// shares one HTTP/2-capable transport, so thousands of concurrent requests
// to a provider are multiplexed over a few connections.
//
// Params of a step (static, from the plan):
//
//	{
//	  "url": "https://api.example.com/v1/chat",
//	  "method": "POST",                  // default POST
//	  "headers": {"Authorization": "Bearer ${env:API_KEY}"},
//	  "body": {"model": "m"},            // merged with the step input (input wins)
//	  "stream": true                     // parse SSE, relay chunks to the live stream
//	}
//
// The step input becomes the JSON request body (merged over params.body).
// Result classification:
//
//	2xx                 -> output = response JSON (or {"status","body"} for non-JSON)
//	429, 502, 503, 504  -> retryable failure (the request did not take effect)
//	other 4xx/5xx       -> failure
//	transport error     -> outcome unknown (the request may have been processed)
package httpexec

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"kairo/task"
)

type Executor struct {
	Client *http.Client
}

// New returns an executor with a transport that speaks HTTP/2 over TLS and
// also cleartext HTTP/2 (h2c) to local gateways.
func New() *Executor {
	var protos http.Protocols
	protos.SetHTTP1(true)
	protos.SetHTTP2(true)
	protos.SetUnencryptedHTTP2(false)
	tr := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		Protocols:           &protos,
	}
	return &Executor{Client: &http.Client{Transport: tr}}
}

// NewH2C returns an executor that uses cleartext HTTP/2 only (prior
// knowledge), for sidecars and local model gateways.
func NewH2C() *Executor {
	var protos http.Protocols
	protos.SetUnencryptedHTTP2(true)
	return &Executor{Client: &http.Client{Transport: &http.Transport{Protocols: &protos, MaxIdleConnsPerHost: 64}}}
}

type params struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    map[string]any    `json:"body"`
	Stream  bool              `json:"stream"`
}

var envRef = regexp.MustCompile(`\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}`)

func expandEnv(s string) string {
	return envRef.ReplaceAllStringFunc(s, func(m string) string {
		return os.Getenv(envRef.FindStringSubmatch(m)[1])
	})
}

func (x *Executor) Execute(ctx context.Context, t *task.Task, emit func([]byte)) task.Result {
	var p params
	if len(t.Params) > 0 {
		if err := json.Unmarshal(t.Params, &p); err != nil {
			return task.Result{Err: "bad params: " + err.Error()}
		}
	}
	if p.URL == "" {
		return task.Result{Err: "params.url is required"}
	}
	if p.Method == "" {
		p.Method = http.MethodPost
	}
	body := map[string]any{}
	for k, v := range p.Body {
		body[k] = v
	}
	if len(t.Input) > 0 {
		var in map[string]any
		if err := json.Unmarshal(t.Input, &in); err == nil {
			for k, v := range in {
				body[k] = v
			}
		}
	}
	var rd io.Reader
	if p.Method != http.MethodGet {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, p.Method, expandEnv(p.URL), rd)
	if err != nil {
		return task.Result{Err: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", t.IdemKey)
	for k, v := range p.Headers {
		req.Header.Set(k, expandEnv(v))
	}
	resp, err := x.Client.Do(req)
	if err != nil {
		// The request may have reached the server.
		return task.Result{Err: err.Error(), Unknown: true}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		msg := fmt.Sprintf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
		switch resp.StatusCode {
		case 429, 502, 503, 504:
			return task.Result{Err: msg, Retryable: true}
		}
		return task.Result{Err: msg}
	}
	if p.Stream && strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		return readSSE(resp.Body, emit)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return task.Result{Err: err.Error(), Unknown: true}
	}
	if json.Valid(b) {
		return task.Result{Output: b, Tokens: usage(b)}
	}
	out, _ := json.Marshal(map[string]any{"status": resp.StatusCode, "body": string(b)})
	return task.Result{Output: out}
}

// readSSE relays each "data:" payload to the live stream and returns the
// last JSON event (which for most LLM APIs carries the final state/usage)
// plus the concatenated text of all data lines.
func readSSE(r io.Reader, emit func([]byte)) task.Result {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	var all strings.Builder
	var last json.RawMessage
	for sc.Scan() {
		line := sc.Bytes()
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		data := bytes.TrimSpace(line[5:])
		if string(data) == "[DONE]" {
			break
		}
		emit(append([]byte(nil), data...))
		if json.Valid(data) {
			last = append(last[:0], data...)
		}
		all.Write(data)
		all.WriteByte('\n')
	}
	if err := sc.Err(); err != nil {
		return task.Result{Err: err.Error(), Unknown: true}
	}
	out, _ := json.Marshal(map[string]any{"last": last, "events": all.String()})
	return task.Result{Output: out, Tokens: usage(last)}
}

func usage(b []byte) int {
	var u struct {
		Usage struct {
			Total  int `json:"total_tokens"`
			Input  int `json:"input_tokens"`
			Output int `json:"output_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal(b, &u) != nil {
		return 0
	}
	if u.Usage.Total > 0 {
		return u.Usage.Total
	}
	return u.Usage.Input + u.Usage.Output
}
