// kairo-wasm exports the pure core (package wasmcore) from a WASM module, for
// SDKs that embed it (ADR 0051). Build:
//
//	GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -o kairo.wasm ./cmd/kairo-wasm
//
// The host calls _initialize once, then passes bytes through memory it
// gets from kairo_alloc. Every call that answers bytes writes them to one
// result buffer (read at kairo_result, valid until the next call) and
// returns their length; a negative length -n means an error, whose message
// is the n bytes there.
//
//	kairo_abi_version() -> version
//	kairo_alloc(n) -> ptr          kairo_free(ptr)
//	kairo_register(specs, len) -> n                 (node specs, a JSON array)
//	kairo_compile(def, len) -> n                    (result: wasmcore.Compiled as JSON)
//	kairo_apply(plan, run, runLen, state, stateLen, event, eventLen, traced) -> n
//	    (result: u32 little-endian length of the new state, the state, then
//	    wasmcore.Result as JSON)
//	kairo_inspect(state, stateLen) -> n             (result: wasmcore.Inspection as JSON, ADR 0060)
//	kairo_result() -> ptr
package main

// main does nothing: a WASM host calls _initialize, then the exports.
// Outside wasip1 the package builds as an empty program.
func main() {}
