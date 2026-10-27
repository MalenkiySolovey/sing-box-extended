package bond

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"
)

type borrowedBondLeg struct {
	outbound.Adapter
	peers chan net.Conn
}

func (b *borrowedBondLeg) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	client, peer := net.Pipe()
	b.peers <- peer
	return client, nil
}

func (*borrowedBondLeg) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}

func TestBondOutboundScopeRetiresBorrowedStreams(t *testing.T) {
	for _, pending := range []bool{true, false} {
		name := "established"
		if pending {
			name = "pending-handshake"
		}
		t.Run(name, func(t *testing.T) {
			leg := &borrowedBondLeg{Adapter: outbound.NewAdapter("borrowed", "canonical", []string{"tcp"}, nil), peers: make(chan net.Conn, 2)}
			registry := outbound.NewRegistry()
			outbound.Register[struct{}](registry, "borrowed", func(context.Context, adapter.Router, log.ContextLogger, string, struct{}) (adapter.Outbound, error) {
				return leg, nil
			})
			ctx := service.ContextWith[adapter.OutboundRegistry](t.Context(), registry)
			logger := log.NewNOPFactory().Logger()
			created, err := NewOutbound(ctx, nil, logger, "parent", option.BondOutboundOptions{Outbounds: []option.BondOutbound{{Outbound: option.Outbound{Type: "borrowed", Options: &struct{}{}}, DownloadRatio: 100, UploadRatio: 100}}})
			if err != nil {
				t.Fatal(err)
			}
			scope := adapter.NewScope(ctx, logger)
			t.Cleanup(func() { _ = scope.Close() })
			if err := created.(*Outbound).Start(adapter.StartStateInitialize, scope); err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			var stream net.Conn
			go func() {
				var err error
				stream, err = created.DialContext(t.Context(), "tcp", M.ParseSocksaddr("example.org:443"))
				result <- err
			}()
			peer := <-leg.peers
			t.Cleanup(func() { _ = peer.Close() })
			if !pending {
				if _, err := ReadRequest(peer); err != nil {
					t.Fatal(err)
				}
				if err := <-result; err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = stream.Close() })
			}
			closed := make(chan error, 1)
			go func() { closed <- scope.Close() }()
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("Bond scope did not retire an admitted dial")
			}
			if pending {
				select {
				case err := <-result:
					if err == nil {
						t.Fatal("retired handshake returned a live Bond stream")
					}
				case <-time.After(250 * time.Millisecond):
					t.Fatal("Bond scope left its handshake blocked on a borrowed leg")
				}
			}
			_ = peer.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
			if _, err := peer.Read(make([]byte, 1)); err == nil {
				t.Fatal("retired Bond stream stayed open")
			} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
				t.Fatal("Bond scope did not close its borrowed stream")
			}
			if conn, err := created.DialContext(t.Context(), "tcp", M.ParseSocksaddr("example.org:443")); err == nil {
				_ = conn.Close()
				t.Fatal("retired Bond parent admitted a new dial")
			}
			// The borrowed canonical adapter remains usable after parent retirement.
			conn, err := leg.DialContext(t.Context(), "tcp", M.Socksaddr{})
			if err != nil {
				t.Fatal(err)
			}
			_ = conn.Close()
			_ = (<-leg.peers).Close()
		})
	}
}
