package mtproxy

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/dolonet/mtg-multi/essentials"
)

type NetworkAdapter struct {
	ctx    context.Context
	dialer essentials.Dialer
}

func NewNetworkAdapter(ctx context.Context, dialer essentials.Dialer) *NetworkAdapter {
	return &NetworkAdapter{ctx, dialer}
}

func (a *NetworkAdapter) Dial(network, address string) (essentials.Conn, error) {
	return a.DialContext(a.ctx, network, address)
}

func (a *NetworkAdapter) DialContext(ctx context.Context, network, address string) (essentials.Conn, error) {
	conn, err := a.dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return essentials.WrapNetConn(conn), nil
}

func (a *NetworkAdapter) MakeHTTPClient(dial func(ctx context.Context, network, address string) (essentials.Conn, error)) *http.Client {
	if dial == nil {
		dial = a.DialContext
	}
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dial(ctx, network, addr)
		}},
	}
}

func (a *NetworkAdapter) NativeDialer() essentials.Dialer {
	return a.dialer
}
