package failover

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/gofrs/uuid/v5"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/common/kmutex"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type echoFailoverRouter struct{ adapter.Router }

func (*echoFailoverRouter) RouteConnectionEx(_ context.Context, conn net.Conn, _ adapter.InboundContext, onClose N.CloseHandlerFunc) {
	go func() { _, err := io.Copy(conn, conn); _ = conn.Close(); onClose(err) }()
}

func TestFailoverNativeSessionTrafficReconnectAndScope(t *testing.T) {
	logger := log.NewNOPFactory().Logger()
	serverScope := adapter.NewScope(t.Context(), logger)
	defer serverScope.Close()
	clientScope := adapter.NewScope(t.Context(), logger)
	defer clientScope.Close()
	h := &Inbound{Adapter: inbound.NewAdapter("failover", "owned-group"), router: &echoFailoverRouter{}, sessionMtx: kmutex.New[uuid.UUID](), conns: make(map[uuid.UUID]*inboundSession), connections: make(map[*inboundLeg]struct{})}
	if err := h.Start(adapter.StartStateInitialize, serverScope); err != nil {
		t.Fatal(err)
	}
	var access sync.Mutex
	var latest net.Conn
	dials := 0
	f := &Failover{ctx: t.Context(), dial: func(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
		client, server := net.Pipe()
		access.Lock()
		latest = client
		dials++
		access.Unlock()
		go func() {
			_ = h.connHandler(ctx, server, adapter.InboundContext{Destination: Destination, User: "member-principal"}, nil)
		}()
		return client, nil
	}}
	if err := f.Start(adapter.StartStateInitialize, clientScope); err != nil {
		t.Fatal(err)
	}
	conn, err := f.DialContext(t.Context(), N.NetworkTCP, M.Socksaddr{Fqdn: "echo.invalid", Port: 443})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	payload := bytes.Repeat([]byte("source-owned-session"), 6000)
	exchange := func() {
		t.Helper()
		written := make(chan error, 1)
		go func() { _, err := conn.Write(payload); written <- err }()
		received := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, received); err != nil {
			t.Fatal("traffic:", err)
		}
		if err := <-written; err != nil {
			t.Fatal("write:", err)
		}
		if !bytes.Equal(received, payload) {
			t.Fatal("reconnect changed stream bytes")
		}
	}
	exchange()
	access.Lock()
	retired := latest
	access.Unlock()
	_ = retired.Close()
	exchange()
	access.Lock()
	count := dials
	access.Unlock()
	if count < 2 {
		t.Fatal("traffic did not exercise reconnect")
	}
	if err := clientScope.Close(); err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() { _, _ = conn.Read(make([]byte, 1)); close(closed) }()
	failoverMustFinish(t, closed)
	if err := serverScope.Close(); err != nil {
		t.Fatal(err)
	}
	h.mtx.RLock()
	remaining := len(h.conns)
	h.mtx.RUnlock()
	if remaining != 0 {
		t.Fatal("retired native sessions remain owned")
	}
}

func TestFailoverCanceledHandshakeRetiresTransport(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	client, peer := net.Pipe()
	defer peer.Close()
	started := make(chan struct{})
	f := &Failover{ctx: t.Context(), dial: func(context.Context, string, M.Socksaddr) (net.Conn, error) { close(started); return client, nil }}
	done := make(chan struct{})
	go func() {
		conn, _ := f.DialContext(ctx, N.NetworkTCP, Destination)
		if conn != nil {
			_ = conn.Close()
		}
		close(done)
	}()
	failoverMustFinish(t, started)
	cancel()
	failoverMustFinish(t, done)
	if _, err := peer.Read(make([]byte, 1)); err == nil {
		t.Fatal("canceled handshake retained its transport")
	}
}
