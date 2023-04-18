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

	// Create db instance
	db, err := mossdb.New(&mossdb.Config{})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Clear db when exit
	defer db.Flush()

	// Set/Get data
	db.Set("key1", []byte("val1"))
	log.Printf("get key1: [%s]", db.Get("key1"))

	// Set data with ttl
	db.Set("key-ttl", []byte("val-ttl"), mossdb.WithTTL(100*time.Millisecond))
	log.Printf("get key-ttl: [%s]", db.Get("key-ttl"))
	time.Sleep(100 * time.Millisecond)
	log.Printf("get key-ttl after expired: [%s]", db.Get("key-ttl"))

	// List kv with prefix
	items := db.List(mossdb.WithPrefixKey("key"))
	log.Printf("list prefix lenght: [%d]", len(items))

	// Transaction success example
	err = db.Tx(func(tx *mossdb.Tx) error {
		val1 := tx.Get("key1")
		tx.Set("key2", val1)

		return nil
	})
	log.Printf("transaction success, key2 should have val, got: [%s], err: [%v]", db.Get("key2"), err)

	// Transaction with error
	err = db.Tx(func(tx *mossdb.Tx) error {
		tx.Set("key3", []byte("val3"))

		return errors.New("some error")
	})
	log.Printf("transaction failed, key3 should be nil, got: [%s], err: [%v]", db.Get("key3"), err)

	// watch example
	Watch(db)

	// Output:
	// 2023/02/23 09:34:18 Start mossdb...
	// 2023/02/23 09:34:18 get key1: [val1]
	// 2023/02/23 09:34:18 get key-ttl: [val-ttl]
	// 2023/02/23 09:34:19 get key-ttl after expired: []
	//
	// 2023/02/23 09:34:19 transaction success, key2 should have val, got: [val1], err: [<nil>]
	// 2023/02/23 09:34:19 transaction failed, key3 should be nil, got: [], err: [some error]
	//
	// 2023/02/23 09:48:50 start watch key watch-key
	// 2023/02/23 09:34:19 receive event: MODIFY, key: watch-key, new val: val1
	// 2023/02/23 09:34:19 receive event: MODIFY, key: watch-key, new val: val2
	// 2023/02/23 09:34:19 receive event: DELETE, key: watch-key, new val:
	// 2023/02/23 09:34:19 context done
}

func Watch(db *mossdb.DB) {
	key := "watch-key"

	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()

	// start watch key
	ch := db.Watch(ctx, key)
	log.Printf("start watch key %s", key)

	go func() {
		db.Set(key, mossdb.Val("val1"))
		db.Set(key, mossdb.Val("val2"))
		db.Delete(key)

		time.Sleep(100 * time.Second)

		db.Set(key, mossdb.Val("val3"))
	}()

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
			log.Printf("receive event: %s, watch id %s,  key: %s, new val: %s", resp.Event.Op, resp.Wid, resp.Event.Key, resp.Event.Val)
		}
	}
}
