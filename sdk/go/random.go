package kairo

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
)

const generatedIDAttempts = 16

type randomSource struct {
	mu sync.Mutex
	r  io.Reader
}

func newRandomSource(r io.Reader) *randomSource {
	if r == nil {
		r = rand.Reader
	}
	return &randomSource{r: r}
}

func (s *randomSource) read(p []byte) error {
	s.mu.Lock()
	_, err := io.ReadFull(s.r, p)
	s.mu.Unlock()
	return err
}

func (s *randomSource) id() (string, error) {
	var b [16]byte
	if err := s.read(b[:]); err != nil {
		return "", fmt.Errorf("kairo: generating id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// token returns a drive lease's cryptographically random token (never 0:
// no lease). The int32 shape is shared with the Python and TypeScript SDKs.
func (s *randomSource) token() (int32, error) {
	for range generatedIDAttempts {
		var b [4]byte
		if err := s.read(b[:]); err != nil {
			return 0, fmt.Errorf("kairo: generating drive token: %w", err)
		}
		if token := int32(binary.BigEndian.Uint32(b[:])); token != 0 {
			return token, nil
		}
	}
	return 0, fmt.Errorf("kairo: generating drive token: %d consecutive zero values", generatedIDAttempts)
}
