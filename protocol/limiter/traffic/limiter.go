package traffic

import (
	"errors"
	"sync"
)

type TrafficLimiter interface {
	Can(n uint64) error
	Add(n uint64) error
}

var errTrafficLimitExceeded = errors.New("traffic limit exceeded")

// A configured budget belongs to this outbound instance. Durable account
// quotas are supplied separately by the manager strategy.
type staticTrafficLimiter struct {
	total uint64
	used  uint64
	mtx   sync.Mutex
}

func (l *staticTrafficLimiter) Can(n uint64) error {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	if n > l.total-l.used {
		return errTrafficLimitExceeded
	}
	return nil
}

func (l *staticTrafficLimiter) Add(n uint64) error {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	if n > l.total-l.used {
		l.used = l.total
		return errTrafficLimitExceeded
	}
	l.used += n
	return nil
}
