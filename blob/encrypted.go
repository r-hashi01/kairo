package blob

import (
	"errors"
	"sync"

	"kairo/seal"
)

// Encrypted wraps a store so objects are sealed (ADR 0021) and their names
// hidden: the stored name is a keyed hash, so it reveals neither run ids
// nor, for content-addressed blobs, a hash of the plaintext.
func Encrypted(inner Store, keys seal.Keys) Store {
	return &encStore{inner: inner, keys: keys, openers: sync.Pool{New: func() any { return seal.NewOpener(keys) }}}
}

type encStore struct {
	inner   Store
	keys    seal.Keys
	openers sync.Pool
}

// ErrNoGroups: the inner store cannot delete groups.
var ErrNoGroups = errors.New("blob: store does not support DeleteGroup")

func aad(name string) []byte { return append([]byte("kairo-blob\x00"), name...) }

// storedName hides key; a grouped key keeps a (hidden) group, so the
// inner store can still delete the group at once.
func (s *encStore) storedName(key string) (string, error) {
	name, err := seal.Name(s.keys, key)
	if err != nil {
		return "", err
	}
	if g, ok := GroupOf(key); ok {
		hg, err := seal.Name(s.keys, g)
		if err != nil {
			return "", err
		}
		return "run/" + hg[2:] + "/" + name, nil
	}
	return name, nil
}

func (s *encStore) DeleteGroup(group string) error {
	g, ok := s.inner.(Grouper)
	if !ok {
		return ErrNoGroups
	}
	hg, err := seal.Name(s.keys, group)
	if err != nil {
		return err
	}
	return g.DeleteGroup("run/" + hg[2:])
}

func (s *encStore) Put(key string, data []byte) error {
	name, err := s.storedName(key)
	if err != nil {
		return err
	}
	b, err := seal.NewBatch(s.keys)
	if err != nil {
		return err
	}
	return s.inner.Put(name, b.Seal(nil, data, aad(key)))
}

func (s *encStore) Get(key string) ([]byte, error) {
	name, err := s.storedName(key)
	if err != nil {
		return nil, err
	}
	env, err := s.inner.Get(name)
	if err != nil {
		return nil, err
	}
	o := s.openers.Get().(*seal.Opener)
	defer s.openers.Put(o)
	return o.Open(env, aad(key))
}

func (s *encStore) Delete(key string) error {
	name, err := s.storedName(key)
	if err != nil {
		return err
	}
	return s.inner.Delete(name)
}
