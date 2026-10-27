package common

import (
	"context"

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
