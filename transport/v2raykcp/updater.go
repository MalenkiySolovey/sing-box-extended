package v2raykcp

import (
	"sync"
	"sync/atomic"
	"time"
)

type Updater struct {
	interval        int64
	shouldContinue  func() bool
	shouldTerminate func() bool
	updateFunc      func()
	notifier        chan struct{}
	mu              sync.Mutex
	closed          bool
	stop            chan struct{}
	workers         sync.WaitGroup
}

func NewUpdater(interval uint32, shouldContinue func() bool, shouldTerminate func() bool, updateFunc func()) *Updater {
	u := &Updater{
		interval:        int64(time.Duration(interval) * time.Millisecond),
		shouldContinue:  shouldContinue,
		shouldTerminate: shouldTerminate,
		updateFunc:      updateFunc,
		notifier:        make(chan struct{}, 1),
		stop:            make(chan struct{}),
	}
	return u
}

func (u *Updater) WakeUp() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed {
		return
	}
	select {
	case u.notifier <- struct{}{}:
		u.workers.Add(1)
		go u.run()
	default:
	}
}

func (u *Updater) run() {
	defer u.workers.Done()
	defer func() {
		<-u.notifier
	}()

	if u.shouldTerminate() {
		return
	}
	ticker := time.NewTicker(u.Interval())
	defer ticker.Stop()

	for u.shouldContinue() {
		u.updateFunc()
		select {
		case <-ticker.C:
		case <-u.stop:
			return
		}
	}
}

func (u *Updater) Stop() {
	u.mu.Lock()
	if !u.closed {
		u.closed = true
		close(u.stop)
	}
	u.mu.Unlock()
	u.workers.Wait()
}

func (u *Updater) Interval() time.Duration {
	return time.Duration(atomic.LoadInt64(&u.interval))
}

func (u *Updater) SetInterval(d time.Duration) {
	atomic.StoreInt64(&u.interval, int64(d))
}
