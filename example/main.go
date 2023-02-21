package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/qingwave/mossdb"
)

func main() {
	log.Printf("Start mossdb...")

	// create db instance
	db, err := mossdb.New(&mossdb.Config{})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// clear db when exit
	// defer db.Flush()

	// set, get data
	db.Set("key1", []byte("val1"))
	log.Printf("get key1: %s", db.Get("key1"))

	// transaction success example
	err = db.Tx(func(tx *mossdb.Tx) error {
		val1 := tx.Get("key1")

		tx.Set("key2", val1)

		return nil
	})
	log.Printf("transaction success, key2 should have val, got: [%s], err: [%v]", db.Get("key2"), err)

	// transaction with error
	err = db.Tx(func(tx *mossdb.Tx) error {
		tx.Set("key3", []byte("val3"))

		return errors.New("some error")
	})
	log.Printf("transaction failed, key3 should be nil, got: [%s], err: [%v]", db.Get("key3"), err)

	// watch example
	Watch(db)
}

func Watch(db *mossdb.DB) {
	key := "watch-key"

	go func() {
		db.Set(key, mossdb.Val("val1"))
		db.Set(key, mossdb.Val("val2"))
		db.Delete(key)

		time.Sleep(100 * time.Second)
		db.Set(key, mossdb.Val("val3"))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	ch := db.Watch(ctx, key)

	for {
		select {
		case <-ctx.Done():
			log.Printf("context done")
			return
		case resp, ok := <-ch:
			if !ok {
				log.Printf("watch done")
				return
			}
			log.Printf("receive event: %s, key: %s, new val: %s", resp.Event.Op, resp.Event.Key, resp.Event.Val)
		}
	}
}
