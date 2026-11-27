package kairo

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

var (
	_ TreeStore = (*MemStore)(nil)
	_ TreeStore = (*SQLStore)(nil)
)

// A recorded event's public form says only what the event recorded, and
// only what applies to its kind (sdk/ts/src/progress.ts's runEvent).
func TestRunEvent(t *testing.T) {
	at := time.UnixMilli(1000)
	for _, tc := range []struct {
		body string
		want RunEvent
	}{
		{`{"kind":"start","at":1000,"act":3,"vars":{"a":1},"meta":{"m":1},"max_steps":9}`, RunEvent{Kind: EventStarted, At: at}},
		{`{"kind":"intent","at":1000,"act":2,"attempt":1}`, RunEvent{Kind: EventDispatched, At: at, Attempt: 1}},
		{`{"kind":"step_ok","at":1000,"act":2,"attempt":2,"data":"x"}`, RunEvent{Kind: EventSucceeded, At: at, Attempt: 2}},
		{`{"kind":"step_err","at":1000,"act":2,"attempt":1,"error":"busy","error_type":"process_lost","retryable":true,"unknown":true,"retry_after_ms":1500}`,
			RunEvent{Kind: EventFailed, At: at, Attempt: 1, Error: "busy", ErrorType: "process_lost", Retryable: true, Unknown: true, RetryAfter: 1500 * time.Millisecond}},
		{`{"kind":"step_wait","at":1000,"act":2,"attempt":1,"deadline":5000}`, RunEvent{Kind: EventWaiting, At: at, Attempt: 1, Deadline: time.UnixMilli(5000)}},
		{`{"kind":"timer","at":1000,"timer":4,"attempt":3,"deadline":5000}`, RunEvent{Kind: EventTimer, At: at}},
		{`{"kind":"signal","at":1000,"name":"approve","data":"yes"}`, RunEvent{Kind: EventSignal, At: at, Signal: "approve"}},
		{`{"kind":"start","at":1000,"name":"not-a-signal"}`, RunEvent{Kind: EventStarted, At: at}},
		{`{"kind":"recover","at":1000,"act":2,"attempt":1}`, RunEvent{Kind: EventRecovered, At: at}},
		{`{"kind":"resolve","at":1000,"act":2,"error":"by hand"}`, RunEvent{Kind: EventResolved, At: at, Error: "by hand"}},
		{`{"kind":"cancel","at":1000,"error":"stop"}`, RunEvent{Kind: EventCancelled, At: at, Error: "stop"}},
		{`{"kind":"something_new","at":1000,"attempt":1}`, RunEvent{Kind: EventOther, At: at}},
	} {
		got, err := runEvent(json.RawMessage(tc.body))
		if err != nil {
			t.Fatalf("%s: %v", tc.body, err)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s:\n got %+v\nwant %+v", tc.body, got, tc.want)
		}
	}
	if _, err := runEvent(json.RawMessage(`{`)); err == nil {
		t.Error("a broken event: no error")
	}
}

func TestProgressKind(t *testing.T) {
	for _, tc := range []struct {
		info       RunInfo
		kind, name string
	}{
		{RunInfo{Plan: planWorkflow, Workflow: "orders"}, "workflow", "orders"},
		{RunInfo{Plan: planCall + "llm"}, "call", "llm"},
		{RunInfo{Plan: planWait + "approve"}, "wait", "approve"},
		{RunInfo{Plan: planWait + "approve@60000"}, "wait", "approve"},
		{RunInfo{Plan: planWait + "a@b"}, "wait", "a@b"},
		{RunInfo{Plan: "graph-plan"}, "run", "graph-plan"},
	} {
		if kind, name := progressKind(tc.info); kind != tc.kind || name != tc.name {
			t.Errorf("%s: %s %s, want %s %s", tc.info.Plan, kind, name, tc.kind, tc.name)
		}
	}
}
