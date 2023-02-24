package ttl

import "fmt"

type Heap[T any] struct {
	data []T
	m    map[string]int
	less func(x, y T) bool
	key  func(T) string
}

func NewHeap[T any](data []T, less func(x, y T) bool, key func(T) string) *Heap[T] {
	h := &Heap[T]{data: data, less: less, key: key, m: map[string]int{}}
	n := h.Len()

	if key != nil {
		for i, t := range h.data {
			h.m[key(t)] = i
		}
	}

	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
	return h
}

func (h *Heap[T]) Push(x T) {
	h.data = append(h.data, x)
	if h.key != nil {
		h.m[h.key(x)] = h.Len() - 1
	}
	h.up(h.Len() - 1)
}

func (h *Heap[T]) Pop() (T, bool) {
	var t T
	if h.Empty() {
		return t, false
	}

	n := h.Len() - 1
	h.Swap(0, n)
	h.down(0, n)

	return h.pop(), true
}

func (h *Heap[T]) pop() T {
	var t T
	item := h.data[h.Len()-1]
	h.data[h.Len()-1] = t // set zero value
	if h.key != nil {
		delete(h.m, h.key(item))
	}
	h.data = h.data[0 : h.Len()-1]
	return item
}

func (h *Heap[T]) Peek() (T, bool) {
	if h.Empty() {
		var t T
		return t, false
	}

	return h.data[0], true
}

func (h *Heap[T]) remove(i int) T {
	if i >= h.Len() {
		var t T
		return t
	}

	n := h.Len() - 1
	if n != i {
		h.Swap(i, n)
		if !h.down(i, n) {
			h.up(i)
		}
	}

	return h.pop()
}

func (h *Heap[T]) Remove(key string) (t T) {
	if h.key == nil {
		return
	}

	return h.remove(h.m[key])
}

func (h *Heap[T]) Update(t T) {
	if h.key == nil {
		return
	}

	i := h.m[h.key(t)]
	h.data[i] = t
	h.Fix(i)
}

func (h *Heap[T]) Fix(i int) {
	if h.Empty() {
		return
	}

	if !h.down(i, h.Len()) {
		h.up(i)
	}
}

func (h *Heap[T]) Len() int {
	return len(h.data)
}

func (h *Heap[T]) Empty() bool {
	return h.Len() == 0
}

func (h *Heap[T]) Less(i, j int) bool {
	return h.less(h.data[i], h.data[j])
}

func (h *Heap[T]) Swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
	if h.key != nil {
		h.m[h.key(h.data[i])] = i
		h.m[h.key(h.data[j])] = j
	}
}

func (h *Heap[T]) Get(key string) (t T) {
	index, ok := h.m[key]
	if ok {
		return h.data[index]
	}
	return
}

func (h *Heap[T]) Scan() []T {
	items := make([]T, len(h.data))
	copy(items, h.data)
	fmt.Println(h.data)
	fmt.Println(h.m)
	return items
}

func (h *Heap[T]) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func (h *Heap[T]) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}
