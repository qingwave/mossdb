package mossdb

import (
	"errors"

	"github.com/qingwave/mossdb/pkg/store"
	"github.com/qingwave/mossdb/pkg/wal"
)

type Tx struct {
	db      *DB
	commits []*Record
	inner   store.Store
}

func (tx *Tx) lock() {
	tx.db.mu.Lock()
}

func (tx *Tx) unlock() {
	tx.db.mu.Unlock()
}

func (db *DB) begin() (*Tx, error) {
	if db.closed {
		return nil, errors.New("mossDB has closed")
	}

	tx := &Tx{
		db:      db,
		commits: make([]*Record, 0),
		inner:   store.NewMap(),
	}

	tx.lock()

	return tx, nil
}

func (tx *Tx) commit() error {
	defer tx.unlock()

	if len(tx.commits) == 0 {
		return nil
	}

	batch := new(wal.Batch)
	for _, commit := range tx.commits {
		data, err := commit.Encode()
		if err != nil {
			return err
		}
		batch.Write(data)
	}

	if n, err := tx.db.wal.WriteBatch(batch, wal.WithSync, wal.WithAtomic); err != nil {
		if n > 0 {
			tx.db.wal.Truncate(n)
		}
		tx.commits = nil
		return err
	}

	// write to memory db
	for _, commit := range tx.commits {
		if err := tx.db.loadRecord(commit); err != nil {
			return err
		}

		tx.db.notify(&Op{
			key: string(commit.Key),
			val: commit.Val,
			op:  OpType(commit.Op),
		})
	}

	tx.commits = nil
	tx.inner = nil

	return nil
}

func (tx *Tx) rollBack() error {
	tx.commits = nil
	tx.inner = nil
	tx.unlock()

	tx.db = nil
	return nil
}

func (tx *Tx) Get(key string, opts ...Option) Val {
	val, ok := tx.inner.Get(key)
	if ok {
		return val
	}

	val, _ = tx.db.store.Get(key)
	return val
}

func (tx *Tx) Set(key string, val Val, opts ...Option) {
	opt := setOption(key, val, opts...)

	tx.commits = append(tx.commits, NewRecord(opt))
	tx.inner.Set(key, val)
}

func (tx *Tx) Delete(key string, opts ...Option) {
	opt := deleteOption(key, opts...)

	tx.commits = append(tx.commits, NewRecord(opt))
	tx.inner.Delete(key)
}

func (db *DB) Tx(f func(tx *Tx) error) error {
	tx, err := db.begin()
	if err != nil {
		return err
	}

	if err := f(tx); err != nil {
		tx.rollBack()
		return err
	}

	return tx.commit()
}
