package mossdb

import (
	"context"
	"time"

	"github.com/qingwave/gocorex/syncx/workqueue"
)

type Watcher struct {
	watchers map[string][]SubWatcher
	queue    workqueue.WorkQueue
}

type SubWatcher struct {
	ctx context.Context
	ch  chan WatchResponse
	opt *Op
}

type WatchResponse struct {
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
		watchers: make(map[string][]SubWatcher),
		queue:    workqueue.New(),
	}
}

func (w *Watcher) Watch(ctx context.Context, key string, opts ...Option) <-chan WatchResponse {
	opt := watchOption(key, opts...)

	ch := make(chan WatchResponse)
	w.watchers[key] = append(w.watchers[key], SubWatcher{
		ctx: ctx,
		ch:  ch,
		opt: opt,
	})

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

		resp := WatchResponse{
			Event: event,
		}

		for i := 0; i < len(w.watchers[event.Key]); {
			wr := w.watchers[event.Key][i]
			if deadline, ok := wr.ctx.Deadline(); ok && deadline.Before(time.Now()) {
				close(wr.ch)
				w.watchers[event.Key] = append(w.watchers[event.Key][:i], w.watchers[event.Key][i+1:]...)
				continue
			}

			wr.ch <- resp
			i++
		}
	}
}

func (w *Watcher) Close() {
	w.queue.Stop()

	for _, watchers := range w.watchers {
		for _, sw := range watchers {
			close(sw.ch)
		}
	}
}
