package store

import (
	"errors"
	"time"
)

var (
	ErrKeyNotExists = errors.New("key not exists")
)

type Store struct {
	kv map[string]Val
}

type Val struct {
	val []byte

	ttl int64
}

func New() *Store {
	return &Store{
		kv: map[string]Val{},
	}
}

func (s *Store) Set(key string, val []byte, ttl int64) {
	s.kv[key] = Val{
		val: val,
		ttl: ttl,
	}
}

func (s *Store) Get(key string) ([]byte, bool) {
	val, ok := s.kv[key]
	if !ok {
		return nil, false
	}

	if val.ttl > 0 && val.ttl <= time.Now().UnixNano() {
		s.Delete(key)
		return nil, false
	}

	return val.val, ok
}

func (s *Store) Expire(key string, ttl int64) error {
	val, ok := s.kv[key]
	if !ok {
		return ErrKeyNotExists
	}

	s.kv[key] = Val{
		val: val.val,
		ttl: ttl,
	}
	return nil
}

func (s *Store) Delete(key string) {
	delete(s.kv, key)
}
