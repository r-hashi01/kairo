package kairo

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"unicode/utf16"
)

// canonical is v as JSON with object keys sorted, byte for byte what the
// TypeScript SDK's canonical makes of the same value (JSON.stringify of
// each leaf, keys in JavaScript's sort order): equal values, equal text,
// in every SDK (ADR 0058).
func canonical(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	var x any
	if err := json.Unmarshal(raw, &x); err != nil {
		return "", err
	}
	var b bytes.Buffer
	writeCanonical(&b, x)
	return b.String(), nil
}

func writeCanonical(b *bytes.Buffer, v any) {
	switch x := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		b.WriteString(strconv.FormatBool(x))
	case float64:
		// encoding/json formats numbers as JavaScript does (ES6 shortest
		// round trip; exponent beyond 1e21 and under 1e-6), except -0.
		if x == 0 {
			b.WriteString("0")
			return
		}
		n, _ := json.Marshal(x)
		b.Write(n)
	case string:
		writeJSString(b, x)
	case []any:
		b.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			writeCanonical(b, e)
		}
		b.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		// JavaScript's default sort: by UTF-16 code units.
		sort.Slice(keys, func(i, j int) bool { return lessUTF16(keys[i], keys[j]) })
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			writeJSString(b, k)
			b.WriteByte(':')
			writeCanonical(b, x[k])
		}
		b.WriteByte('}')
	default:
		panic(fmt.Sprintf("kairo: canonical: unexpected %T", v))
	}
}

// writeJSString writes s as JSON.stringify does: only ", \ and control
// characters are escaped (not <, >, & or U+2028/U+2029, as encoding/json
// would).
func writeJSString(b *bytes.Buffer, s string) {
	const hexd = "0123456789abcdef"
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 0x20 {
				b.WriteString(`\u00`)
				b.WriteByte(hexd[r>>4])
				b.WriteByte(hexd[r&0xf])
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
}

func lessUTF16(a, b string) bool {
	ua, ub := utf16.Encode([]rune(a)), utf16.Encode([]rune(b))
	for i := 0; i < len(ua) && i < len(ub); i++ {
		if ua[i] != ub[i] {
			return ua[i] < ub[i]
		}
	}
	return len(ua) < len(ub)
}

// callID is a call's run id (ADR 0049): the workflow's id, a hash of the
// call's kind and input, and how many such calls came before.
func callID(workflow, kind, canon string, n int) string {
	h := sha256.Sum256([]byte(kind + "\x00" + canon))
	return workflow + "/" + hex.EncodeToString(h[:])[:24] + "." + strconv.Itoa(n)
}
