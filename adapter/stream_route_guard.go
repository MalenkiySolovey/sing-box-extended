package adapter

import (
	"context"
	"errors"
)

// ErrUnguardedStreamAction reports a route that cannot dispatch directly to
// a guarded outbound.
var ErrUnguardedStreamAction = errors.New("stream route action cannot be guarded")

// StreamRouteGuard can veto a TCP stream after routing and before outbound
// dispatch. The caller must separately qualify any changes made by the outbound.
type StreamRouteGuard func(InboundContext, Outbound) error

func ContextWithStreamRouteGuard(ctx context.Context, guard StreamRouteGuard) context.Context {
	return context.WithValue(ctx, streamRouteGuardKey{}, guard)
}

func StreamRouteGuardFromContext(ctx context.Context) StreamRouteGuard {
	guard, _ := ctx.Value(streamRouteGuardKey{}).(StreamRouteGuard)
	return guard
}

type streamRouteGuardKey struct{}
