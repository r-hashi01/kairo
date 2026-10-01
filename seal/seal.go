// Package seal encrypts and authenticates what kairo stores (ADR 0021):
// log records, snapshots and blobs, with AES-256-GCM.
//
// Nonces: GCM must never reuse a (key, nonce) pair, and a busy log writes
// far more than the ~2^32 messages that random 96-bit nonces allow per key.
// So every sealing operation (one log batch, one object) draws a random
// 128-bit salt and derives a one-off subkey with HKDF-SHA256 from the
// master key; within that subkey the nonce is the item's index. Subkeys
// collide only if salts do (2^-128 per pair).
//
// Envelope (v1): 0x01 | key id (uvarint) | salt (16) | index (uvarint) |
// ciphertext+tag. The associated data binds the envelope to its place
// (log id + LSN, or object name), so moving, reordering or forging items
// is detected.
package seal

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
)

// ErrTampered is returned when stored data fails authentication or is
// out of place. Callers must not continue from such data.
var ErrTampered = errors.New("seal: stored data failed authentication (tampered or corrupt)")

// Keys provides the master keys. Implementations may fetch them from a KMS
// or a secret manager; kairo never stores keys.
type Keys interface {
	// Current returns the key new data is sealed with.
	Current() (id uint32, key []byte, err error)
	// Key returns the key with the given id, for reading older data.
	Key(id uint32) ([]byte, error)
	// NameKey returns the long-lived key that hides object names. It is
	// not rotated: changing it makes stored objects unreachable.
	NameKey() ([]byte, error)
}

// Static is a Keys held in memory.
type Static struct {
	current uint32
	keys    map[uint32][]byte
	name    []byte
}

// NewStatic validates 32-byte keys. current must be one of keys.
func NewStatic(current uint32, keys map[uint32][]byte, nameKey []byte) (*Static, error) {
	if _, ok := keys[current]; !ok {
		return nil, fmt.Errorf("seal: current key %d not provided", current)
	}
	for id, k := range keys {
		if len(k) != 32 {
			return nil, fmt.Errorf("seal: key %d must be 32 bytes", id)
		}
	}
	if len(nameKey) < 32 {
		return nil, errors.New("seal: name key must be at least 32 bytes")
	}
	return &Static{current: current, keys: keys, name: nameKey}, nil
}

func (s *Static) Current() (uint32, []byte, error) { return s.current, s.keys[s.current], nil }
func (s *Static) NameKey() ([]byte, error)         { return s.name, nil }
func (s *Static) Key(id uint32) ([]byte, error) {
	k, ok := s.keys[id]
	if !ok {
		return nil, fmt.Errorf("seal: unknown key %d", id)
	}
	return k, nil
}

const (
	version  = 1
	saltSize = 16
)

var info = []byte("kairo seal v1")

func subkey(master, salt []byte) (cipher.AEAD, error) {
	k, err := hkdf.Key(sha256.New, master, salt, string(info), 32)
	if err != nil {
		return nil, err
	}
	b, err := aes.NewCipher(k)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(b)
}

func nonce(index uint32) []byte {
	n := make([]byte, 12)
	binary.BigEndian.PutUint32(n[8:], index)
	return n
}

// Batch seals several items under one salt (one log batch).
type Batch struct {
	keyID uint32
	salt  [saltSize]byte
	aead  cipher.AEAD
	next  uint32
}

// NewBatch starts a sealing batch with the current key.
func NewBatch(keys Keys) (*Batch, error) {
	id, master, err := keys.Current()
	if err != nil {
		return nil, err
	}
	b := &Batch{keyID: id}
	if _, err := rand.Read(b.salt[:]); err != nil {
		return nil, err
	}
	if b.aead, err = subkey(master, b.salt[:]); err != nil {
		return nil, err
	}
	return b, nil
}

// Seal appends the envelope of plain (bound to aad) to dst.
func (b *Batch) Seal(dst, plain, aad []byte) []byte {
	i := b.next
	b.next++
	dst = append(dst, version)
	dst = binary.AppendUvarint(dst, uint64(b.keyID))
	dst = append(dst, b.salt[:]...)
	dst = binary.AppendUvarint(dst, uint64(i))
	return b.aead.Seal(dst, nonce(i), plain, aad)
}

// Opener opens envelopes, caching the subkey of the last salt seen (items
// of one batch are read consecutively). Not safe for concurrent use.
type Opener struct {
	keys     Keys
	lastKey  uint32
	lastSalt [saltSize]byte
	aead     cipher.AEAD
}

func NewOpener(keys Keys) *Opener { return &Opener{keys: keys} }

// Open returns the plaintext of env, which must have been sealed with aad.
func (o *Opener) Open(env, aad []byte) ([]byte, error) {
	if len(env) < 1 || env[0] != version {
		return nil, ErrTampered
	}
	r := env[1:]
	id, n := binary.Uvarint(r)
	if n <= 0 || len(r) < n+saltSize {
		return nil, ErrTampered
	}
	r = r[n:]
	var salt [saltSize]byte
	copy(salt[:], r)
	r = r[saltSize:]
	idx, n := binary.Uvarint(r)
	if n <= 0 || idx > 1<<32-1 {
		return nil, ErrTampered
	}
	r = r[n:]
	if o.aead == nil || o.lastKey != uint32(id) || o.lastSalt != salt {
		master, err := o.keys.Key(uint32(id))
		if err != nil {
			return nil, err
		}
		if o.aead, err = subkey(master, salt[:]); err != nil {
			return nil, err
		}
		o.lastKey, o.lastSalt = uint32(id), salt
	}
	plain, err := o.aead.Open(nil, nonce(uint32(idx)), r, aad)
	if err != nil {
		return nil, ErrTampered
	}
	return plain, nil
}

// Name hides an object name: a keyed hash, so stored names reveal neither
// run ids nor (for content-addressed blobs) a hash of the plaintext.
func Name(keys Keys, name string) (string, error) {
	k, err := keys.NameKey()
	if err != nil {
		return "", err
	}
	m := hmac.New(sha256.New, k)
	m.Write([]byte(name))
	return "h:" + hex.EncodeToString(m.Sum(nil)), nil
}
