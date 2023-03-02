package mossdb

import (
	"errors"

	"github.com/qingwave/mossdb/pkg/wal"
)

type Tx struct {
	db      *DB
	commits []*Record
	undos   []*Record
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
		undos:   make([]*Record, 0),
	}

	tx.lock()

	return tx, nil
}

func (tx *Tx) commit() error {
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

	// notify all commit
	for _, commit := range tx.commits {
		tx.db.notify(&Op{
			key: string(commit.Key),
			val: commit.Val,
			op:  OpType(commit.Op),
		})
	}

	tx.commits = nil
	tx.undos = nil
	tx.unlock()

	return nil
}

func (tx *Tx) rollBack() error {
	tx.commits = nil

	for _, undo := range tx.undos {
		if err := tx.db.loadRecord(undo); err != nil {
			return err
		}
	}

	tx.unlock()

	tx.undos = nil
	tx.db = nil
	return nil
}

func (tx *Tx) Get(key string, opts ...Option) Val {
	val, _ := tx.db.store.Get(key)
	return val
}

func (tx *Tx) List(opts ...Option) map[string][]byte {
	op := listOption(opts...)

	if op.all {
		return tx.db.store.Dump()
	}

	if op.prefix && op.key != "" {
		return tx.db.store.Prefix(op.key)
	}

	return make(map[string][]byte)
}

func (tx *Tx) Set(key string, val Val, opts ...Option) {
	op := setOption(key, val, opts...)

	undo := &Op{key: key}
	old, ok := tx.db.store.Get(key)
	if ok {
		undo.op = ModifyOp
		undo.val = old
	} else {
		undo.op = DeleteOp
	}

	tx.undos = append(tx.undos, NewRecord(undo))
	tx.commits = append(tx.commits, NewRecord(op))

	tx.db.set(op)
}

func (tx *Tx) Delete(key string, opts ...Option) {
	op := deleteOption(key, opts...)

	undo := &Op{key: key}
	old, ok := tx.db.store.Get(key)
	if ok {
		undo.op = ModifyOp
		undo.val = old
		tx.undos = append(tx.undos, NewRecord(undo))
	} else {
		return
	}

	tx.commits = append(tx.commits, NewRecord(op))

	tx.db.delete(op)
}

func (db *DB) Tx(f func(tx *Tx) error) error {
	tx, err := db.begin()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tx.rollBack()
		}
	}()

	if err = f(tx); err != nil {
		return err
	}

	err = tx.commit()

	return err
}
