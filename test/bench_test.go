package test

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/qingwave/mossdb"
	"github.com/qingwave/mossdb/pkg/store"
)

var (
	db      *mossdb.DB
	testDir string
	randVal string
)

func init() {
	testDir = "testdata/mossdb"
	if err := os.RemoveAll(testDir); err != nil {
		panic(err)
	}

	if err := os.MkdirAll(testDir, os.ModePerm); err != nil {
		panic(err)
	}

	buf := make([]byte, 64)
	io.ReadFull(rand.Reader, buf)
	randVal = string(buf)

	var err error
	db, err = mossdb.New(&mossdb.Config{
		Path:  testDir,
		Store: store.NewRadixTree(),
	})
	if err != nil {
		panic(err)
	}
}

func initDB() {
	for i := 0; i < 10000; i++ {
		key := string(getKey(i))
		if err := db.Set(key, getVal()); err != nil {
			panic(err)
		}
	}
}

func BenchmarkMossDBSet(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := getKey(i)
		if err := db.Set(key, getVal()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMossDBGet(b *testing.B) {
	initDB()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := getKey(1)
		if val := db.Get(key); val == nil {
			b.Fatal(errors.New("failed to get key"))
		}
	}
}

func BenchmarkMossDBList(b *testing.B) {
	initDB()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if val := db.List(mossdb.WithPrefixKey("test_key101")); len(val) == 0 {
			b.Fatal(errors.New("failed to list key"))
		}
	}
}

func BenchmarkMossDBTx(b *testing.B) {
	initDB()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := getKey(1)
		db.Tx(func(tx *mossdb.Tx) error {
			val := tx.Get(key)
			if val == nil {
				b.Fatal(errors.New("failed to get key"))
			}

			tx.Set(fmt.Sprintf("new_%d", i), val)
			return nil
		})
	}
}

func getKey(i int) string {
	return fmt.Sprintf("test_key%d", i)
}

func getVal() []byte {
	return []byte(randVal)
}
