package ttl

import (
	"sync/atomic"
	"time"
)

type Job struct {
	Key      string
	Schedule time.Time
}

func New(exec Runner) *TTL {
	return &TTL{
		started: &atomic.Bool{},
		event:   make(chan struct{}),
		exec:    exec,
		jobs: NewHeap(make([]*Job, 0), func(x, y *Job) bool {
			return x.Schedule.Before(y.Schedule)
		}, func(job *Job) string {
			return job.Key
		}),
	}
}

type Runner func(string) error

type TTL struct {
	started *atomic.Bool
	event   chan struct{}

	exec Runner

	jobs *Heap[*Job]
}

func (ttl *TTL) Add(job *Job) {
	if job == nil {
		return
	}

	ttl.jobs.Push(job)
	ttl.notify()
}

func (ttl *TTL) Delete(key string) {
	ttl.jobs.Remove(key)
	ttl.notify()
}

func (ttl *TTL) Update(job *Job) {
	if job == nil || job.Key == "" {
		return
	}

	ttl.jobs.Update(job)
	ttl.notify()
}

func (ttl *TTL) Run() error {
	ttl.started.Store(true)

	for {
		if !ttl.started.Load() {
			break
		}

		ttl.run()
	}

	return nil
}

func (ttl *TTL) Stop() {
	ttl.started.Store(false)
	close(ttl.event)
}

func (ttl *TTL) IsExpired(key string) bool {
	job := ttl.jobs.Get(key)
	return job != nil && !job.Schedule.After(time.Now())
}

const infTime time.Duration = 1<<63 - 1

func (ttl *TTL) run() {
	now := time.Now()
	duration := infTime
	job, ok := ttl.jobs.Peek()
	if ok {
		if job.Schedule.After(now) {
			duration = job.Schedule.Sub(now)
		} else {
			duration = 0
		}
	}

	if duration > 0 {
		timer := time.NewTimer(duration)
		defer timer.Stop()

		select {
		case <-ttl.event:
			return
		case <-timer.C:
		}
	}

	job, ok = ttl.jobs.Pop()
	if !ok {
		return
	}

	go ttl.exec(job.Key)
}

func (ttl *TTL) notify() {
	if ttl.started.Load() {
		ttl.event <- struct{}{}
	}
}
