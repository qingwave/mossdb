package mossdb

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qingwave/gocorex/containerx"
	"github.com/qingwave/gocorex/syncx/workqueue"
	"github.com/qingwave/mossdb/pkg/store/art"
)

const DefaultSendTimeout = 1 * time.Second

type Watcher struct {
	mu       sync.RWMutex
	watchers map[string]*SubWatcher
	keys     map[string]containerx.Set[string]
	ranges   *art.Tree
	queue    workqueue.WorkQueue
	stop     chan struct{}
	autoId   *atomic.Uint64
}

type SubWatcher struct {
	wid      string
	ctx      context.Context
	ch       chan WatchResponse
	opt      *Op
	canceled bool
}

type WatchResponse struct {
	Wid      string
	Event    *WatchEvent
	Canceled bool
}

type WatchEvent struct {
	Key    string
	Val    Val
	OldVal Val
	Op     OpType
}

func NewWatcher() *Watcher {
	return &Watcher{
		watchers: make(map[string]*SubWatcher),
		keys:     make(map[string]containerx.Set[string]),
		ranges:   art.New(),
		queue:    workqueue.New(),
		stop:     make(chan struct{}),
		autoId:   &atomic.Uint64{},
	}
}

func (w *Watcher) Watch(ctx context.Context, key string, opts ...Option) <-chan WatchResponse {
	opt := watchOption(key, opts...)

	ch := make(chan WatchResponse)
	wid := w.watchId()
	subWatcher := &SubWatcher{
		wid: wid,
		ctx: ctx,
		ch:  ch,
		opt: opt,
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.watchers[wid] = subWatcher
	if opt.prefix {
		bkey := []byte(key)
		items, ok := w.ranges.Search(bkey).(containerx.Set[string])
		if !ok {
			items = containerx.NewSet(wid)
		} else {
			items.Insert(wid)
		}
		w.ranges.Insert(bkey, items)
	} else {
		if _, ok := w.keys[key]; ok {
			w.keys[key].Insert(wid)
		} else {
			w.keys[key] = containerx.NewSet(wid)
		}
	}

	go func() {
		select {
		case <-ctx.Done():
			w.mu.Lock()
			w.evict(subWatcher)
			w.mu.Unlock()
		case <-w.stop:
			return
		}
	}()

	return ch
}

func (w *Watcher) AddEvent(event *WatchEvent) {
	w.queue.Write(event)
}

func (w *Watcher) Run() {
	for {
		data, ok := w.queue.Read()
		if !ok {
			break
		}

		event, ok := data.(*WatchEvent)
		if !ok {
			continue
		}

		w.mu.RLock()
		for _, sw := range w.watchersByKey(event.Key) {
			sw := sw
			if sw.canceled {
				continue
			}

			go func() {
				timeout := DefaultSendTimeout
				if sw.opt.watchSendTimeout > 0 {
					timeout = sw.opt.watchSendTimeout
				}
				timer := time.NewTimer(timeout)
				defer timer.Stop()

				select {
				// send event
				case sw.ch <- WatchResponse{
					Wid:   sw.wid,
					Event: event,
				}:
				// watch ctx done
				case <-sw.ctx.Done():
				// send event timeout
				case <-timer.C:
				}
			}()
		}
		w.mu.RUnlock()
	}
}

func (w *Watcher) Close() {
	w.queue.Stop()

	close(w.stop)

	w.mu.Lock()
	defer w.mu.Unlock()
	for _, sw := range w.watchers {
		w.evict(sw)
	}
}

func (w *Watcher) evict(sw *SubWatcher) error {
	if sw == nil {
		return nil
	}
	if !sw.canceled {
		close(sw.ch)
		sw.canceled = true
	}

	if sw.opt.prefix {
		bkey := []byte(sw.opt.key)
		items, ok := w.ranges.Search(bkey).(containerx.Set[string])
		if ok {
			items.Delete(sw.wid)
			if items.Len() == 0 {
				w.ranges.Delete(bkey)
			}
		}
	} else {
		items, ok := w.keys[sw.opt.key]
		if ok {
			items.Delete(sw.wid)
			if items.Len() == 0 {
				delete(w.keys, sw.opt.key)
			}
		}
	}

	delete(w.watchers, sw.wid)

	return nil
}

func (w *Watcher) watchersByKey(key string) []*SubWatcher {
	wids := make([]string, 0)
	items, ok := w.keys[key]
	if ok {
		wids = append(wids, items.Slice()...)
	}

	w.ranges.Stab([]byte(key), func(node *art.Node) {
		items, ok := node.Value().(containerx.Set[string])
		if ok {
			wids = append(wids, items.Slice()...)
		}
	})

	watchers := make([]*SubWatcher, 0, len(wids))
	for _, wid := range wids {
		sw, ok := w.watchers[wid]
		if ok {
			watchers = append(watchers, sw)
		}
	}

	return watchers
}

func (w *Watcher) watchId() string {
	return strconv.FormatUint(w.autoId.Add(1), 10)
}
