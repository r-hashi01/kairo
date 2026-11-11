package httpaction

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/r-hashi01/kairo/task"
)

func TestCallbackCapabilityAuthenticatesAttemptAndRotatesKeys(t *testing.T) {
	now := time.Unix(100, 0)
	oldKey := []byte(strings.Repeat("o", 32))
	newKey := []byte(strings.Repeat("n", 32))
	old, err := NewCallbackCapabilities("https://callbacks.example/kairo", [][]byte{oldKey}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	taskIn := &task.Task{RunID: "r1", StepID: "call[2]", Action: "send", Act: 7, Attempt: 3}
	callback := old.Issue(taskIn, now.Add(time.Minute).UnixMilli())
	token := callback.URL[strings.LastIndex(callback.URL, "/")+1:]

	rotated, err := NewCallbackCapabilities("https://callbacks.example/kairo", [][]byte{newKey, oldKey}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	claims, err := rotated.Verify(token)
	if err != nil {
		t.Fatal(err)
	}
	if claims.RunID != taskIn.RunID || claims.Act != taskIn.Act || claims.Attempt != taskIn.Attempt || claims.Action != taskIn.Action || claims.StepID != taskIn.StepID {
		t.Fatalf("claims = %+v", claims)
	}

	tampered := token[:len(token)-1] + map[bool]string{true: "A", false: "B"}[token[len(token)-1] == 'A']
	if _, err := rotated.Verify(tampered); !errors.Is(err, ErrCapability) {
		t.Fatalf("tampered token: %v", err)
	}
	now = now.Add(time.Minute)
	if _, err := rotated.Verify(token); !errors.Is(err, ErrCapabilityExpired) {
		t.Fatalf("expired token: %v", err)
	}
}

func TestCallbackCapabilityRejectsUnsafeConfiguration(t *testing.T) {
	key := []byte(strings.Repeat("k", 32))
	for _, base := range []string{"http://callbacks.example", "https://user@example.com", "https://example.com?x=1"} {
		if _, err := NewCallbackCapabilities(base, [][]byte{key}, nil); !errors.Is(err, ErrCapability) {
			t.Fatalf("base %q: %v", base, err)
		}
	}
	if _, err := NewCallbackCapabilities("https://example.com", [][]byte{[]byte("short")}, nil); !errors.Is(err, ErrCapability) {
		t.Fatalf("short key: %v", err)
	}
	if _, err := NewCallbackCapabilities("http://127.0.0.1:8421", [][]byte{key}, nil); err != nil {
		t.Fatalf("loopback: %v", err)
	}
}
