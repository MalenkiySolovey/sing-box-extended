package quic

// Solovey integration on Shtorm d9442d14b19ddd02605bf3ee893a171947038e19.
// The consuming module pairs this seam with the adapted sing-quic parent owner.
// Shtorm registers its Close after the UDP listener in a reverse-order Scope.

import (
	"context"
	"errors"
	"sync"

	"github.com/sagernet/quic-go/http3"
	"github.com/sagernet/sing-quic"
)

// Private type prevents publishing these unauthenticated transport handles as
// Hysteria/TUIC principal sessions. HTTP/3 may carry several request principals.
type naiveParent struct{}

type http3Lifecycle struct {
	parents   *qtls.ParentOwner[naiveParent]
	server    *http3.Server
	closeOnce sync.Once
	closeErr  error
}

func (l *http3Lifecycle) Close() error {
	l.closeOnce.Do(func() {
		// This owner fences acceptance, closes each exact parent and drains the
		// completed queue before the embedding inbound can destroy its UDP.
		l.closeErr = l.parents.Close()
		// ServeQUICConn handlers receive parent cancellation. Bound their server
		// cleanup separately; a timeout is an error, never a delivery guarantee.
		ctx, cancel := context.WithTimeout(context.Background(), qtls.ParentDrainTimeout)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- l.server.Close() }()
		select {
		case err := <-done:
			l.closeErr = errors.Join(l.closeErr, err)
		case <-ctx.Done():
			l.closeErr = errors.Join(l.closeErr, ctx.Err())
		}
	})
	return l.closeErr
}
