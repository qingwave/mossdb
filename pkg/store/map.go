package store

type Map struct {
	kv map[string][]byte
}

func NewMap() Store {
	return &Map{
		kv: map[string][]byte{},
	}
}

func (m *Map) Set(key string, val []byte) {
	m.kv[key] = val
}

func (m *Map) Get(key string) ([]byte, bool) {
	val, ok := m.kv[key]
	if !ok {
		return nil, false
	}

	return val, true
}

func (m *Map) Delete(key string) {
	delete(m.kv, key)
}

func (m *Map) Len() int {
	return len(m.kv)
}

func (m *Map) Range(start, end string) map[string][]byte {
	items := make(map[string][]byte)
	for k, v := range m.kv {
		if k >= start && k < end {
			items[k] = v
		}
	}
	return items
}

func (m *Map) Prefix(key string) map[string][]byte {
	return m.Range(key, GetPrefixEnd(key))
}

func (m *Map) Dump() map[string][]byte {
	items := make(map[string][]byte)
	for k, v := range m.kv {
		items[k] = v
	}
	return items
}
