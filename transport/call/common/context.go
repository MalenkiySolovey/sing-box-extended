package common

import (
	"context"
	"time"

	N "github.com/sagernet/sing/common/network"
)

// ContextForDialer keeps platform DNS and relay workers inside an optional
// serving owner. Standalone upstream dialers retain their existing lifetime.
func ContextForDialer(d N.Dialer) context.Context {
	if serving, ok := d.(interface{ Context() context.Context }); ok {
		return serving.Context()
	}
	return context.Background()
}

func RetireServingTransports(d N.Dialer) {
	if serving, ok := d.(interface{ CloseServingTransports() }); ok {
		serving.CloseServingTransports()
	}
}

func WaitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
