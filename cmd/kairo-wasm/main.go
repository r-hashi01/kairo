//go:build wasip1

// The exports; see doc.go.

package main

import (
	"encoding/binary"
	"encoding/json"
	"unsafe"

	"kairo/wasmcore"
)

var (
	c    = wasmcore.New()
	out  []byte
	bufs = map[uint32][]byte{}
)

// bytesAt is n bytes at p, a buffer from kairo_alloc (nil if it is not
// one, or too short).
func bytesAt(p, n uint32) []byte {
	if n == 0 {
		return nil
	}
	b := bufs[p]
	if uint32(len(b)) < n {
		return nil
	}
	return b[:n]
}

func fail(err error) int32 {
	out = append(out[:0], err.Error()...)
	return -int32(len(out))
}

//go:wasmexport kairo_abi_version
func abiVersion() int32 { return wasmcore.ABIVersion }

//go:wasmexport kairo_alloc
func alloc(n uint32) uint32 {
	b := make([]byte, max(n, 1))
	p := uint32(uintptr(unsafe.Pointer(&b[0])))
	bufs[p] = b
	return p
}

//go:wasmexport kairo_free
func free(p uint32) { delete(bufs, p) }

//go:wasmexport kairo_result
func result() uint32 {
	if len(out) == 0 {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(&out[0])))
}

//go:wasmexport kairo_register
func register(p, n uint32) int32 {
	if err := c.Register(bytesAt(p, n)); err != nil {
		return fail(err)
	}
	out = out[:0]
	return 0
}

//go:wasmexport kairo_compile
func compile(p, n uint32) int32 {
	r, err := c.Compile(bytesAt(p, n))
	if err != nil {
		return fail(err)
	}
	b, _ := json.Marshal(r)
	out = append(out[:0], b...)
	return int32(len(out))
}

//go:wasmexport kairo_apply
func apply(plan int32, rp, rn, sp, sn, ep, en uint32, traced int32) int32 {
	state, r, err := c.Apply(int(plan), string(bytesAt(rp, rn)), bytesAt(sp, sn), bytesAt(ep, en), traced != 0)
	if err != nil {
		return fail(err)
	}
	b, err := json.Marshal(r)
	if err != nil {
		return fail(err)
	}
	out = binary.LittleEndian.AppendUint32(out[:0], uint32(len(state)))
	out = append(out, state...)
	out = append(out, b...)
	return int32(len(out))
}
