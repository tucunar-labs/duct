package web

import (
	"container/list"
	"sync"
)

// lru is a small bounded cache of chart payloads: at most maxEntries entries
// and maxBytes of payload, least recently used out first. Keys carry the lake
// snapshot, so a new snapshot simply never hits the old entries and they age out.
type lru struct {
	mu         sync.Mutex
	maxEntries int
	maxBytes   int
	bytes      int
	ll         *list.List
	items      map[string]*list.Element
}

type lruEntry struct {
	key string
	val *chartData
}

func newLRU(maxEntries, maxBytes int) *lru {
	return &lru{maxEntries: maxEntries, maxBytes: maxBytes, ll: list.New(), items: map[string]*list.Element{}}
}

func (c *lru) get(key string) (*chartData, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[key]; ok {
		c.ll.MoveToFront(e)
		return e.Value.(*lruEntry).val, true
	}
	return nil, false
}

func (c *lru) put(key string, v *chartData) {
	size := v.size()
	if size > c.maxBytes {
		return // never evict everything for one huge result
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[key]; ok {
		c.bytes -= e.Value.(*lruEntry).val.size()
		e.Value.(*lruEntry).val = v
		c.bytes += size
		c.ll.MoveToFront(e)
	} else {
		c.items[key] = c.ll.PushFront(&lruEntry{key, v})
		c.bytes += size
	}
	for c.ll.Len() > c.maxEntries || c.bytes > c.maxBytes {
		e := c.ll.Back()
		ent := e.Value.(*lruEntry)
		c.ll.Remove(e)
		delete(c.items, ent.key)
		c.bytes -= ent.val.size()
	}
}

func (c *lru) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}
