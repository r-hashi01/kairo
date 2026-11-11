package httpaction

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/r-hashi01/kairo/task"
)

const callbackPath = "/v1/action-results/"

var (
	ErrCapability        = errors.New("callback capability")
	ErrCapabilityExpired = errors.New("callback capability expired")
)

// CallbackClaims are the attempt and deadline authenticated by a daemon-owned
// callback URL (ADR 0070). Action and StepID let the engine preserve the usual
// blob and trace handling without a separate callback lookup table.
type CallbackClaims struct {
	RunID   string `json:"r"`
	Act     uint32 `json:"a"`
	Attempt int32  `json:"n"`
	Until   int64  `json:"u"`
	Action  string `json:"x"`
	StepID  string `json:"s"`
}

func (c CallbackClaims) Task() *task.Task {
	return &task.Task{RunID: c.RunID, Act: c.Act, Attempt: c.Attempt, Action: c.Action, StepID: c.StepID}
}

// CallbackCapabilities issues and verifies stateless, attempt-scoped bearer
// URLs. The first key signs new URLs; every key verifies during rotation.
type CallbackCapabilities struct {
	base string
	keys [][]byte
	now  func() time.Time
}

func NewCallbackCapabilities(base string, keys [][]byte, now func() time.Time) (*CallbackCapabilities, error) {
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("%w: callback base is not a plain absolute URL", ErrCapability)
	}
	local := u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"
	if u.Scheme != "https" && !(u.Scheme == "http" && local) {
		return nil, fmt.Errorf("%w: callback base must use https (http is only for loopback)", ErrCapability)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("%w: at least one key is required", ErrCapability)
	}
	copyKeys := make([][]byte, len(keys))
	for i, key := range keys {
		if len(key) < sha256.Size {
			return nil, fmt.Errorf("%w: key %d has less than 256 bits", ErrCapability, i)
		}
		copyKeys[i] = append([]byte(nil), key...)
	}
	if now == nil {
		now = time.Now
	}
	return &CallbackCapabilities{base: strings.TrimRight(base, "/"), keys: copyKeys, now: now}, nil
}

func capabilityMAC(key, payload []byte) []byte {
	m := hmac.New(sha256.New, key)
	_, _ = m.Write(payload)
	return m.Sum(nil)
}

// Issue makes the callback carried by one worker task.
func (c *CallbackCapabilities) Issue(t *task.Task, until int64) *task.Callback {
	claims := CallbackClaims{RunID: t.RunID, Act: t.Act, Attempt: t.Attempt, Until: until, Action: t.Action, StepID: t.StepID}
	payload, _ := json.Marshal(claims)
	token := base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(capabilityMAC(c.keys[0], payload))
	return &task.Callback{URL: c.base + callbackPath + token, Until: until}
}

// Verify authenticates token with the current or a previous rotation key.
func (c *CallbackCapabilities) Verify(token string) (CallbackClaims, error) {
	var claims CallbackClaims
	if len(token) == 0 || len(token) > 16<<10 {
		return claims, ErrCapability
	}
	p, m, ok := strings.Cut(token, ".")
	if !ok || strings.Contains(m, ".") {
		return claims, ErrCapability
	}
	payload, err := base64.RawURLEncoding.DecodeString(p)
	if err != nil {
		return claims, ErrCapability
	}
	mac, err := base64.RawURLEncoding.DecodeString(m)
	if err != nil || len(mac) != sha256.Size {
		return claims, ErrCapability
	}
	valid := false
	for _, key := range c.keys {
		valid = hmac.Equal(mac, capabilityMAC(key, payload)) || valid
	}
	if !valid || json.Unmarshal(payload, &claims) != nil || claims.RunID == "" || claims.Action == "" || claims.StepID == "" || claims.Attempt <= 0 || claims.Until <= 0 {
		return CallbackClaims{}, ErrCapability
	}
	if c.now().UnixMilli() >= claims.Until {
		return CallbackClaims{}, ErrCapabilityExpired
	}
	return claims, nil
}
