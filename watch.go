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
	"github.com/qingwave/mossdb/pkg/utils"
)

const DefaultWatcherQueueSize = 1024

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
	new      chan struct{}
	events   *utils.Ring[*WatchEvent]
	opt      *Op
	canceled bool
}

func (sw *SubWatcher) PushEvent(e *WatchEvent) {
	sw.events.PushBack(e)
	select {
	case sw.new <- struct{}{}:
	default:
	}
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
	eventQueueSize := DefaultWatcherQueueSize
	if opt.eventBuffSize > 0 {
		eventQueueSize = opt.eventBuffSize
	}

	ch := make(chan WatchResponse)
	wid := w.watchId()
	sw := &SubWatcher{
		wid:    wid,
		ctx:    ctx,
		new:    make(chan struct{}),
		ch:     ch,
		events: utils.NewRing[*WatchEvent](eventQueueSize),
		opt:    opt,
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.watchers[wid] = sw
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
		for {
			if sw.canceled {
				return
			}

			var tickc <-chan time.Time

			event := sw.events.Front()
			if event != nil {
				if sw.send(event) {
					sw.events.PopFront()
					continue
				} else {
					tickc = time.After(10 * time.Millisecond)
				}
			}

			select {
			case <-sw.new:
			case <-tickc:
			case <-ctx.Done():
				w.mu.Lock()
				w.evict(sw)
				w.mu.Unlock()
				return
			case <-w.stop:
				return
			}
		}
	}()

	return ch
}

func (sw *SubWatcher) send(event *WatchEvent) bool {
	select {
	case sw.ch <- WatchResponse{
		Wid:   sw.wid,
		Event: event,
	}:
		return true
	default:
		return false
	}
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
			if sw.canceled {
				continue
			}

			sw.PushEvent(event)
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
		sw.canceled = true
		close(sw.new)
		close(sw.ch)
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
