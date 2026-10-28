package masque

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
)

type lifetimeMASQUEClient struct {
	closed atomic.Bool
	dials  atomic.Int32
}

func (c *lifetimeMASQUEClient) Close() error { c.closed.Store(true); return nil }
func (c *lifetimeMASQUEClient) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	c.dials.Add(1)
	return nil, net.ErrClosed
}
func (c *lifetimeMASQUEClient) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	c.dials.Add(1)
	return nil, net.ErrClosed
}

func TestMASQUEBootstrapBelongsToServingScope(t *testing.T) {
	for _, pending := range []bool{true, false} {
		name := "initialized"
		if pending {
			name = "pending-bootstrap"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			logger := log.NewNOPFactory().Logger()
			created, err := NewOutbound(ctx, nil, logger, "parent", option.MASQUEOutboundOptions{UseL4Proxy: true})
			if err != nil {
				t.Fatal(err)
			}
			w := created.(*Outbound)
			client := &lifetimeMASQUEClient{}
			admitted := make(chan struct{})
			// The real bootstrap uses w.ctx for Cloudflare requests and may publish a
			// client as cancellation completes. Keep that boundary deterministic here.
			w.startHandler = func() {
				defer close(w.await)
				close(admitted)
				if pending {
					<-w.ctx.Done()
				}
				w.client = client
			}
			scope := adapter.NewScope(ctx, logger)
			if err := w.Start(adapter.StartStateInitialize, scope); err != nil {
				t.Fatal(err)
			}
			if err := w.Start(adapter.StartStatePostStart, scope); err != nil {
				t.Fatal(err)
			}
			<-admitted
			if !pending {
				<-w.await
			}
			done := make(chan error, 1)
			go func() { done <- scope.Close() }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				cancel()
				<-done
				t.Fatal("MASQUE scope waited on uncancelled bootstrap")
			}
			if !client.closed.Load() {
				t.Fatal("retired MASQUE scope left its published client open")
			}
			_, _ = w.DialContext(context.Background(), "tcp", M.ParseSocksaddr("192.0.2.1:443"))
			if client.dials.Load() != 0 {
				t.Fatal("retired MASQUE parent delegated a new dial")
			}
		})
	}
}
