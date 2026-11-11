package core

import (
	"encoding/json"
	"testing"
)

func TestSignalOutputKeepsPayloadAndReceiptMetadata(t *testing.T) {
	got := signalOutput(json.RawMessage(`{"by":"alice"}`), json.RawMessage(`{"id":"signal-1","early":true,"only_if_waiting":false}`))
	var out struct {
		Payload json.RawMessage `json:"payload"`
		Signal  struct {
			ID    string `json:"id"`
			Early bool   `json:"early"`
		} `json:"_kairo_signal"`
	}
	if err := json.Unmarshal(got, &out); err != nil {
		t.Fatal(err)
	}
	if string(out.Payload) != `{"by":"alice"}` || out.Signal.ID != "signal-1" || !out.Signal.Early {
		t.Fatalf("signal output %s", got)
	}
}
