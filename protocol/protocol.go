// Package protocol is the executor protocol: workers in any language
// connect over a UNIX socket (or TCP) and pull tasks.
//
// Framing: every message is a 4-byte big-endian length, a 1-byte type and a
// JSON body. Flow control is credit based: the worker announces how many
// tasks it can hold and grants one credit back per finished task, so the
// runtime never pushes more work than a worker asked for.
//
//	worker -> runtime   Hello  {"worker":"py-1","actions":["code.run"],"credit":8,"token":"..."}
//	runtime -> worker   Task   {task}
//	worker -> runtime   Chunk  {"seq":17,"data":"<base64>"}      (live output)
//	worker -> runtime   Result {"seq":17,"output":{...}}          (also grants 1 credit)
//	                           {"seq":17,"wait":{"until":<ms>,"output":{...}}} (ADR 0045)
//	worker -> runtime   Credit {"n":4}                            (optional extra credit)
//	runtime -> worker   Cancel {"seq":17}                         (the step was abandoned)
//	runtime -> worker   RunEnd {"run_id":"r1"}                    (the run finished)
//
// Cancel is a request (ADR 0026): the worker should stop the task and must
// still send its Result, which releases the task's concurrency slot and
// credit. Workers that predate Cancel skip it like any unknown type.
//
// RunEnd goes only to workers whose Hello asked for it ("run_end": true),
// once per run they were sent a task of, after those tasks (ADR 0044): a
// worker that keeps something per run can let it go. It is not sent for
// runs whose tasks were sent before a restart or a reconnection, so a
// worker must cope without it.
//
// A server with a Token accepts only workers whose Hello carries it
// (ADR 0037); others are disconnected without an answer.
package protocol

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"

	"github.com/r-hashi01/kairo/task"
)

type MsgType byte

const (
	MsgHello  MsgType = 1
	MsgTask   MsgType = 2
	MsgResult MsgType = 3
	MsgCredit MsgType = 4
	MsgChunk  MsgType = 5
	MsgCancel MsgType = 6
	MsgRunEnd MsgType = 7
)

const maxFrame = 64 << 20

type Hello struct {
	Worker  string   `json:"worker"`
	Actions []string `json:"actions"`
	Credit  int      `json:"credit"`
	Token   string   `json:"token,omitempty"`
	// RunEnd asks for a RunEnd per run the worker was sent a task of.
	RunEnd bool `json:"run_end,omitempty"`
}

type Result struct {
	Seq       uint64          `json:"seq"`
	Output    json.RawMessage `json:"output,omitempty"`
	Err       string          `json:"error,omitempty"`
	Retryable bool            `json:"retryable,omitempty"`
	Unknown   bool            `json:"unknown,omitempty"`
	Tokens    int             `json:"tokens,omitempty"`
	ErrType   string          `json:"error_type,omitempty"`
	Meta      json.RawMessage `json:"meta,omitempty"`
	// RateLimited: the destination refused the task for its limits (ADR 0039).
	RateLimited bool `json:"rate_limited,omitempty"`
	// Wait: wait until a deadline, then end with an output (ADR 0045).
	Wait *task.Wait `json:"wait,omitempty"`
}

type Cancel struct {
	Seq uint64 `json:"seq"`
}

type RunEnd struct {
	RunID string `json:"run_id"`
}

type Credit struct {
	N int `json:"n"`
}

type Chunk struct {
	Seq  uint64 `json:"seq"`
	Data []byte `json:"data"`
}

var ErrFrameTooLarge = errors.New("protocol: frame too large")

func WriteFrame(w *bufio.Writer, t MsgType, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(len(body)+1))
	hdr[4] = byte(t)
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(body)
	return err
}

func ReadFrame(r *bufio.Reader) (MsgType, []byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 || n > maxFrame {
		return 0, nil, ErrFrameTooLarge
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, err
	}
	return MsgType(buf[0]), buf[1:], nil
}
