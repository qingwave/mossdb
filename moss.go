package mossdb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/qingwave/mossdb/pkg/store"
	"github.com/qingwave/mossdb/pkg/ttl"
	"github.com/qingwave/mossdb/pkg/wal"
)

func New(conf *Config) (*DB, error) {
	conf = conf.Default()
	wal, err := wal.Open(conf.WalPath, nil)
	if err != nil {
		return nil, err
	}
	db := &DB{
		store:   conf.Store,
		wal:     wal,
		watcher: NewWatcher(),
	}

	db.ttl = ttl.New(func(key string) error {
		return db.Delete(key, WithMsg("Expired"))
	})

	if err := db.loadWal(); err != nil {
		return nil, err
	}

	go db.watcher.Run()
	go db.ttl.Run()

	return db, nil
}

type Config struct {
	WalPath string
	Store   store.Store
}

func (c *Config) Default() *Config {
	if c == nil {
		c = &Config{}
	}
	if c.WalPath == "" {
		c.WalPath = "moss"
	}
	if c.Store == nil {
		c.Store = store.NewRadixTree()
	}
	return c
}

type Val []byte

type DB struct {
	mu      sync.RWMutex
	store   store.Store
	closed  bool
	wal     *wal.Log
	watcher *Watcher
	ttl     *ttl.TTL
}

func (db *DB) Close() {
	db.watcher.Close()
	db.ttl.Stop()

	if db.wal != nil {
		db.wal.Close()
	}

	db.closed = true
}

func (db *DB) Get(key string, opts ...Option) Val {
	_ = getOption(key, opts...)

	db.mu.RLock()
	defer db.mu.RUnlock()

	val, ok := db.store.Get(key)
	if !ok {
		return nil
	}

	if db.ttl.IsExpired(key) {
		return nil
	}

	return val
}

func (db *DB) List(opts ...Option) map[string][]byte {
	op := listOption(opts...)

	db.mu.RLock()
	defer db.mu.RUnlock()

	if op.all {
		return db.store.Dump()
	}

	if op.prefix && op.key != "" {
		return db.store.Prefix(op.key)
	}

	return make(map[string][]byte)
}

func (db *DB) Set(key string, val Val, opts ...Option) error {
	op := setOption(key, val, opts...)

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.buildAndStoreRecord(op); err != nil {
		return err
	}

	db.set(op)

	db.notify(op)

	return nil
}

func (db *DB) set(op *Op) {
	db.store.Set(op.key, op.val)
	if op.ttl > 0 {
		db.ttl.Add(&ttl.Job{
			Key:      op.key,
			Schedule: time.Unix(0, op.ttl),
		})
	}
}

func (db *DB) Delete(key string, opts ...Option) error {
	op := deleteOption(key, opts...)

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.buildAndStoreRecord(op); err != nil {
		return err
	}

	db.delete(op)

	db.notify(op)

	return nil
}

func (db *DB) delete(op *Op) {
	db.store.Delete(op.key)
	db.ttl.Delete(op.key)
}

func (db *DB) Watch(ctx context.Context, key string, opts ...Option) <-chan WatchResponse {
	return db.watcher.Watch(ctx, key, opts...)
}

func (db *DB) Flush() error {
	if err := db.wal.Flush(); err != nil {
		return nil
	}
	db.store = store.NewMap()
	return nil
}

func (db *DB) loadWal() error {
	if db.wal == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	for i := 1; i <= db.wal.Segments(); i++ {
		for j := 0; ; j++ {
			data, err := db.wal.Read(uint64(i), uint64(j))
			if err != nil {
				if err == wal.ErrEOF {
					break
				}
				return err
			}

			record, err := Decode(data)
			if err != nil {
				return err
			}

			db.loadRecord(record)
		}
	}

	return nil
}

func (db *DB) loadRecord(record *Record) error {
	op := OpFromRecord(record)
	switch record.Op {
	case uint16(ModifyOp):
		db.set(op)
	case uint16(DeleteOp):
		db.delete(op)
	default:
		return fmt.Errorf("invalid operation %d", record.Op)
	}
	return nil
}

func (db *DB) buildAndStoreRecord(op *Op) error {
	record := NewRecord(op)
	data, err := record.Encode()
	if err != nil {
		return err
	}

	n, err := db.wal.Write(data)
	if err != nil {
		if n > 0 {
			db.wal.Truncate(n)
		}
		return err
	}

	return nil
}

func (db *DB) notify(op *Op) {
	if op == nil || !op.IsMutate() {
		return
	}

	db.watcher.AddEvent(&WatchEvent{
		Key: op.key,
		Val: op.val,
		Op:  op.op,
	})
}
