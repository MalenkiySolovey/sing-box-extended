package bond

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

type fixtureInbound struct{ inbound.Adapter }

func (*fixtureInbound) Start(adapter.StartStage, *adapter.Scope) error { return nil }

type routedBond struct {
	conn     net.Conn
	metadata adapter.InboundContext
}
type fixtureRouter struct {
	adapter.Router
	routed  chan routedBond
	onRoute func(net.Conn)
}

func (r *fixtureRouter) RouteConnectionEx(_ context.Context, c net.Conn, m adapter.InboundContext, _ N.CloseHandlerFunc) {
	r.routed <- routedBond{c, m}
	if r.onRoute != nil {
		r.onRoute(c)
	}
}

func bondFixture(t *testing.T) (*Inbound, *adapter.Scope, *fixtureRouter) {
	t.Helper()
	registry := inbound.NewRegistry()
	inbound.Register[struct{}](registry, "fixture", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ struct{}) (adapter.Inbound, error) {
		return &fixtureInbound{inbound.NewAdapter("fixture", tag)}, nil
	})
	ctx := service.ContextWith[adapter.InboundRegistry](t.Context(), registry)
	router := &fixtureRouter{routed: make(chan routedBond, 8)}
	logger := log.NewNOPFactory().Logger()
	created, err := NewInbound(ctx, router, logger, "bond-root", option.BondInboundOptions{Inbounds: []option.Inbound{{Type: "fixture", Tag: "leg"}}})
	if err != nil {
		t.Fatal(err)
	}
	h := created.(*Inbound)
	scope := adapter.NewScope(ctx, logger)
	t.Cleanup(func() { _ = scope.Close() })
	for _, stage := range []adapter.StartStage{adapter.StartStateInitialize, adapter.StartStateStart} {
		if err := h.Start(stage, scope); err != nil {
			t.Fatal(err)
		}
	}
	return h, scope, router
}

func admitLeg(t *testing.T, h *Inbound, request Request, callbacks *atomic.Int32) (net.Conn, error) {
	t.Helper()
	server, client := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	result := make(chan error, 1)
	onClose := N.OnceClose(func(error) { callbacks.Add(1) })
	go func() {
		err := h.connHandler(t.Context(), server, adapter.InboundContext{User: "fixture-user"}, onClose)
		if err != nil {
			N.CloseOnHandshakeFailure(server, onClose, err)
		}
		result <- err
	}()
	_ = client.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err := client.Write(requestWire(t, request)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		return client, err
	case <-time.After(time.Second):
		t.Fatal("Bond leg admission did not return")
		return nil, nil
	}
}

func requirePeerClosed(t *testing.T, peer net.Conn) {
	t.Helper()
	_ = peer.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	_, err := peer.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("retired Bond leg remained open")
	}
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("retired Bond leg retained its peer")
	}
}

func TestBondScopeClosesCompletedBundleAndNotifiesEveryLeg(t *testing.T) {
	h, scope, router := bondFixture(t)
	request := Request{UUID: uuid.Must(uuid.NewV4()), Count: 2, DownloadRatio: 50, UploadRatio: 50, Destination: M.ParseSocksaddr("example.test:443")}
	var callbacks atomic.Int32
	first, err := admitLeg(t, h, request, &callbacks)
	if err != nil {
		t.Fatal(err)
	}
	request.Index = 1
	second, err := admitLeg(t, h, request, &callbacks)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case routed := <-router.routed:
		if routed.metadata.Inbound != "bond-root" || routed.metadata.InboundType != "bond" || routed.metadata.Destination != request.Destination {
			t.Fatal("Bond route lost root inbound identity/destination")
		}
	case <-time.After(time.Second):
		t.Fatal("completed bundle did not route")
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	if callbacks.Load() != 2 {
		t.Error("Scope did not notify both accepted leg owners exactly once")
	}
	requirePeerClosed(t, first)
	requirePeerClosed(t, second)
}

func TestBondRejectsDuplicateAndConflictingLegs(t *testing.T) {
	for _, conflict := range []string{"duplicate", "count", "destination"} {
		t.Run(conflict, func(t *testing.T) {
			h, scope, router := bondFixture(t)
			request := Request{UUID: uuid.Must(uuid.NewV4()), Count: 2, DownloadRatio: 50, UploadRatio: 50, Destination: M.ParseSocksaddr("example.test:443")}
			var callbacks atomic.Int32
			first, err := admitLeg(t, h, request, &callbacks)
			if err != nil {
				t.Fatal(err)
			}
			switch conflict {
			case "count":
				request.Index, request.Count = 1, 3
			case "destination":
				request.Index, request.Destination = 1, M.ParseSocksaddr("other.test:443")
			}
			second, err := admitLeg(t, h, request, &callbacks)
			if err == nil {
				t.Error("accepted duplicate/conflicting Bond leg")
			}
			_ = scope.Close()
			requirePeerClosed(t, first)
			requirePeerClosed(t, second)
			if callbacks.Load() != 2 {
				t.Error("discarded/partial legs lost their close notifications")
			}
			select {
			case <-router.routed:
				t.Error("conflicting bundle reached routing")
			default:
			}
		})
	}
}

type observingConn struct {
	net.Conn
	reading chan struct{}
	once    sync.Once
}

func (c *observingConn) Read(p []byte) (int, error) {
	c.once.Do(func() { close(c.reading) })
	return c.Conn.Read(p)
}

func TestBondScopeRetiresPendingHandshakeAndRejectsLateAdmission(t *testing.T) {
	h, scope, _ := bondFixture(t)
	server, client := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	observed := &observingConn{Conn: server, reading: make(chan struct{})}
	done := make(chan struct{})
	var callbacks atomic.Int32
	go func() {
		_ = h.connHandler(t.Context(), observed, adapter.InboundContext{}, func(error) { callbacks.Add(1) })
		close(done)
	}()
	select {
	case <-observed.reading:
	case <-time.After(time.Second):
		t.Fatal("header read never started")
	}
	_ = scope.Close()
	select {
	case <-done:
	case <-time.After(150 * time.Millisecond):
		t.Fatal("Scope left a Bond header read blocked")
	}
	if callbacks.Load() != 1 {
		t.Fatal("pending handshake close notification was lost")
	}
	request := Request{Count: 1, DownloadRatio: 100, UploadRatio: 100, Destination: M.ParseSocksaddr("example.test:443")}
	// A retired owner must reject before reading any new header.
	lateServer, latePeer := net.Pipe()
	t.Cleanup(func() { _ = lateServer.Close(); _ = latePeer.Close() })
	late := make(chan error, 1)
	go func() {
		late <- h.connHandler(t.Context(), lateServer, adapter.InboundContext{Destination: request.Destination}, nil)
	}()
	select {
	case err := <-late:
		if err == nil {
			t.Fatal("late leg accepted")
		}
	case <-time.After(150 * time.Millisecond):
		t.Fatal("retired owner began another handshake")
	}
}

func TestBondLegCloseCanReenterScopeCleanup(t *testing.T) {
	h, scope, router := bondFixture(t)
	router.onRoute = func(conn net.Conn) { _ = conn.Close() }
	server, peer := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = peer.Close() })
	request := Request{UUID: uuid.Must(uuid.NewV4()), Count: 1, DownloadRatio: 100, UploadRatio: 100, Destination: M.ParseSocksaddr("example.test:443")}
	done := make(chan struct{})
	go func() {
		_ = h.connHandler(t.Context(), server, adapter.InboundContext{}, func(error) { _ = scope.Close() })
		close(done)
	}()
	_ = peer.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err := peer.Write(requestWire(t, request)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("leg callback deadlocked while retiring its serving Scope")
	}
}

func TestBondRoutesBidirectionalTrafficAndRetiresItWithScope(t *testing.T) {
	h, scope, router := bondFixture(t)
	request := Request{UUID: uuid.Must(uuid.NewV4()), Count: 2, DownloadRatio: 50, UploadRatio: 25, Destination: M.ParseSocksaddr("example.test:443")}
	var callbacks atomic.Int32
	first, err := admitLeg(t, h, request, &callbacks)
	if err != nil {
		t.Fatal(err)
	}
	request.Index, request.UploadRatio = 1, 75
	second, err := admitLeg(t, h, request, &callbacks)
	if err != nil {
		t.Fatal(err)
	}
	routed := <-router.routed
	client := NewBondedConn([]net.Conn{first, second}, []uint8{25, 75}, []uint8{50, 50})
	_ = client.SetDeadline(time.Now().Add(3 * time.Second))
	_ = routed.conn.SetDeadline(time.Now().Add(3 * time.Second))
	echoDone := make(chan struct{})
	go func() { _, _ = io.Copy(routed.conn, routed.conn); close(echoDone) }()
	payload := bytes.Repeat([]byte("Bond live asymmetric traffic\x00"), 6000)
	wrote := make(chan error, 1)
	go func() { _, err := client.Write(payload); wrote <- err }()
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatal("Bond live echo:", err)
	}
	if err := <-wrote; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, got) {
		t.Fatal("Bond live traffic corrupted")
	}
	_ = scope.Close()
	select {
	case <-echoDone:
	case <-time.After(time.Second):
		t.Fatal("Scope retained routed Bond traffic")
	}
	if callbacks.Load() != 2 {
		t.Fatal("routed legs were not closed exactly once")
	}
}

func TestBondPendingEvictionClosesItsLeg(t *testing.T) {
	h, _, _ := bondFixture(t)
	request := Request{UUID: uuid.Must(uuid.NewV4()), Count: 2, DownloadRatio: 50, UploadRatio: 50, Destination: M.ParseSocksaddr("example.test:443")}
	var callbacks atomic.Int32
	peer, err := admitLeg(t, h, request, &callbacks)
	if err != nil {
		t.Fatal(err)
	}
	// Invoke the same eviction callback as expiry without waiting for the real
	// TCP connect timeout or replacing the dependency's implementation.
	h.conns.Delete(request.UUID)
	requirePeerClosed(t, peer)
	if callbacks.Load() != 1 {
		t.Fatal("pending eviction lost the leg close callback")
	}
}

func TestBondRejectsOverflowingBundleRatiosAndClosesAllLegs(t *testing.T) {
	h, _, router := bondFixture(t)
	request := Request{UUID: uuid.Must(uuid.NewV4()), Count: 4, DownloadRatio: 89, UploadRatio: 89, Destination: M.ParseSocksaddr("example.test:443")}
	var callbacks atomic.Int32
	var peers []net.Conn
	for index := range 4 {
		request.Index = byte(index)
		peer, err := admitLeg(t, h, request, &callbacks)
		peers = append(peers, peer)
		if index < 3 && err != nil {
			t.Fatal(err)
		}
		if index == 3 && err == nil {
			t.Fatal("356-percent ratios wrapped into a valid bundle")
		}
	}
	for _, peer := range peers {
		requirePeerClosed(t, peer)
	}
	if callbacks.Load() != 4 {
		t.Fatal("invalid bundle retained legs or lost callbacks")
	}
	select {
	case <-router.routed:
		t.Fatal("invalid ratios reached routing")
	default:
	}
}
