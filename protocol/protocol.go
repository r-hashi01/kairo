// Package protocol is the executor protocol: workers in any language
// connect over a UNIX socket (or TCP) and pull tasks.
//
// Framing: every message is a 4-byte big-endian length, a 1-byte type and a
// JSON body. Flow control is credit based: the worker announces how many
// tasks it can hold and grants one credit back per finished task, so the
// runtime never pushes more work than a worker asked for.
//
//	worker -> runtime   Hello  {"worker":"py-1","actions":["code.run"],"credit":8}
//	runtime -> worker   Task   {task}
//	worker -> runtime   Chunk  {"seq":17,"data":"<base64>"}      (live output)
//	worker -> runtime   Result {"seq":17,"output":{...}}          (also grants 1 credit)
//	worker -> runtime   Credit {"n":4}                            (optional extra credit)
package protocol

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
)

type MsgType byte

const (
	MsgHello  MsgType = 1
	MsgTask   MsgType = 2
	MsgResult MsgType = 3
	MsgCredit MsgType = 4
	MsgChunk  MsgType = 5
)

const maxFrame = 64 << 20

type Hello struct {
	Worker  string   `json:"worker"`
	Actions []string `json:"actions"`
	Credit  int      `json:"credit"`
}

type Result struct {
	Seq       uint64          `json:"seq"`
	Output    json.RawMessage `json:"output,omitempty"`
	Err       string          `json:"error,omitempty"`
	Retryable bool            `json:"retryable,omitempty"`
	Unknown   bool            `json:"unknown,omitempty"`
	Tokens    int             `json:"tokens,omitempty"`
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
