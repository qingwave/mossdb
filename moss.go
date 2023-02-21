package mossdb

import (
	"context"
	"fmt"
	"sync"

	"github.com/qingwave/mossdb/pkg/store"
	"github.com/qingwave/mossdb/pkg/wal"
)

func New(conf *Config) (*DB, error) {
	conf = conf.Default()
	wal, err := wal.Open(conf.WalPath, nil)
	if err != nil {
		return nil, err
	}
	db := &DB{
		store:   store.New(),
		wal:     wal,
		watcher: NewWatcher(),
	}

	if err := db.loadWal(); err != nil {
		return nil, err
	}

	go db.watcher.Run()

	return db, nil
}

type Config struct {
	WalPath string
}

func (c *Config) Default() *Config {
	if c == nil {
		c = &Config{}
	}
	if c.WalPath == "" {
		c.WalPath = "moss"
	}
	return c
}

type Val []byte

type DB struct {
	mu      sync.RWMutex
	store   *store.Store
	closed  bool
	wal     *wal.Log
	watcher *Watcher
}

func (db *DB) Close() {
	if db.wal != nil {
		db.wal.Close()
	}

	db.watcher.Close()

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

	return val
}

func (db *DB) Set(key string, val Val, opts ...Option) error {
	opt := setOption(key, val, opts...)

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.buildAndStoreRecord(opt); err != nil {
		return err
	}

	db.store.Set(key, val, opt.ttl)

	db.notify(opt)

	return nil
}

func (db *DB) Delete(key string, opts ...Option) error {
	opt := deleteOption(key, opts...)

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.buildAndStoreRecord(opt); err != nil {
		return err
	}

	db.store.Delete(key)

	db.notify(opt)

	return nil
}

func (db *DB) Watch(ctx context.Context, key string, opts ...Option) <-chan WatchResponse {
	return db.watcher.Watch(ctx, key, opts...)
}

func (db *DB) Flush() error {
	if err := db.wal.Flush(); err != nil {
		return nil
	}
	db.store = store.New()
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
	switch record.Op {
	case uint16(ModifyOp):
		db.store.Set(string(record.Key), record.Val, int64(record.TTL))
	case uint16(DeleteOp):
		db.store.Delete(string(record.Key))
	default:
		return fmt.Errorf("invalid operation %d", record.Op)
	}
	return nil
}

func (db *DB) buildAndStoreRecord(opt *Op) error {
	record := NewRecord(opt)
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

func (db *DB) notify(opt *Op) {
	if opt == nil || !opt.IsMutate() {
		return
	}

	db.watcher.AddEvent(&WatchEvent{
		Key: opt.key,
		Val: opt.val,
		Op:  opt.op,
	})
}
