package store

import "github.com/qingwave/mossdb/pkg/store/art"

type RadixTree struct {
	tree *art.Tree
}

func NewRadixTree() Store {
	return &RadixTree{
		tree: art.New(),
	}
}

func (r *RadixTree) Get(key string) ([]byte, bool) {
	val := r.tree.Search([]byte(key))
	if data, ok := val.([]byte); ok {
		return data, true
	}

	return nil, false
}

func (r *RadixTree) Set(key string, val []byte) {
	r.tree.Insert([]byte(key), val)
}

func (r *RadixTree) Delete(key string) {
	r.tree.Delete([]byte(key))
}

func (r *RadixTree) Prefix(key string) map[string][]byte {
	items := make(map[string][]byte)
	r.tree.Scan([]byte(key), func(node *art.Node) {
		val, ok := node.Value().([]byte)
		if ok {
			items[string(node.Key())] = val
		}
	})
	return items
}

func (r *RadixTree) Dump() map[string][]byte {
	items := make(map[string][]byte)
	r.tree.Each(func(node *art.Node) {
		val, ok := node.Value().([]byte)
		if ok {
			items[string(node.Key())] = val
		}
	})
	return items
}

func (r *RadixTree) Len() int {
	return int(r.tree.Size())
}
