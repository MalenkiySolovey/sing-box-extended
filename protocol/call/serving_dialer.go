package call

import (
	"context"
	"io"
	"net"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/dialer"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// Platform HTTP clients also use contexts detached from Connect. Own their
// transports so serving cancellation interrupts pending auth and signaling I/O.
type servingDialer struct {
	N.Dialer
	ctx         context.Context
	access      sync.Mutex
	closed      bool
	connections map[io.Closer]struct{}
}

func newServingDialer(ctx context.Context, upstream N.Dialer) *servingDialer {
	return &servingDialer{Dialer: upstream, ctx: ctx, connections: make(map[io.Closer]struct{})}
}

// ICE and platform DNS paths inspect ResolveDialer. Preserve that contract only
// when the selected upstream actually implements it; keep network I/O owned.
func (d *servingDialer) transportDialer() N.Dialer {
	if resolver, ok := d.Dialer.(dialer.ResolveDialer); ok {
		return &servingResolveDialer{servingDialer: d, resolver: resolver}
	}
	return d
}

type servingResolveDialer struct {
	*servingDialer
	resolver dialer.ResolveDialer
}

func (d *servingResolveDialer) QueryOptions() adapter.DNSQueryOptions {
	return d.resolver.QueryOptions()
}

func (d *servingDialer) dialContext(ctx context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(d.ctx, cancel)
	if d.ctx.Err() != nil {
		cancel()
	}
	return ctx, func() { stop(); cancel() }
}

func (d *servingDialer) retain(conn io.Closer) error {
	d.access.Lock()
	defer d.access.Unlock()
	if d.closed || d.ctx.Err() != nil {
		return net.ErrClosed
	}
	d.connections[conn] = struct{}{}
	return nil
}

func (d *servingDialer) forget(conn io.Closer) {
	d.access.Lock()
	delete(d.connections, conn)
	d.access.Unlock()
}

func (d *servingDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	ctx, cancel := d.dialContext(ctx)
	defer cancel()
	conn, err := d.Dialer.DialContext(ctx, network, destination)
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}
		return nil, err
	}
	owned := &servingConn{Conn: conn, owner: d}
	if err := ctx.Err(); err != nil {
		_ = owned.Close()
		return nil, err
	}
	if err = d.retain(owned); err != nil {
		_ = owned.Close()
		return nil, err
	}
	return owned, nil
}

func (d *servingDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	ctx, cancel := d.dialContext(ctx)
	defer cancel()
	conn, err := d.Dialer.ListenPacket(ctx, destination)
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}
		return nil, err
	}
	owned := &servingPacketConn{PacketConn: conn, owner: d}
	if err := ctx.Err(); err != nil {
		_ = owned.Close()
		return nil, err
	}
	if err = d.retain(owned); err != nil {
		_ = owned.Close()
		return nil, err
	}
	return owned, nil
}

func (d *servingDialer) Close() {
	d.access.Lock()
	d.closed = true
	connections := make([]io.Closer, 0, len(d.connections))
	for conn := range d.connections {
		connections = append(connections, conn)
	}
	d.access.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
	}
}

type servingConn struct {
	net.Conn
	owner *servingDialer
	once  sync.Once
}

func (c *servingConn) Close() error {
	closed := false
	c.once.Do(func() { closed = true })
	if !closed {
		return nil
	}
	c.owner.forget(c)
	return c.Conn.Close()
}

type servingPacketConn struct {
	net.PacketConn
	owner *servingDialer
	once  sync.Once
}

func (c *servingPacketConn) Close() error {
	closed := false
	c.once.Do(func() { closed = true })
	if !closed {
		return nil
	}
	c.owner.forget(c)
	return c.PacketConn.Close()
}
