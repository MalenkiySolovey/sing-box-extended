package failover

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
	"github.com/sagernet/sing-box/common/kmutex"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type observedFailoverConn struct {
	net.Conn
	read, write         chan struct{}
	readOnce, writeOnce sync.Once
	onClose             func()
}

func (c *observedFailoverConn) Read(b []byte) (int, error) {
	if c.read != nil {
		c.readOnce.Do(func() { close(c.read) })
	}
	return c.Conn.Read(b)
}
func (c *observedFailoverConn) Write(b []byte) (int, error) {
	if c.write != nil {
		c.writeOnce.Do(func() { close(c.write) })
	}
	return c.Conn.Write(b)
}

func (c *observedFailoverConn) Close() error {
	err := c.Conn.Close()
	if c.onClose != nil {
		c.onClose()
	}
	return err
}

func failoverMustFinish(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("failover lifecycle remained blocked")
	}
}

func TestFailoverScopeJoinsOutboundRetirement(t *testing.T) {
	logger := log.NewNOPFactory().Logger()
	scope := adapter.NewScope(t.Context(), logger)
	t.Cleanup(func() { _ = scope.Close() })
	raw, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	observed := &observedFailoverConn{Conn: raw, onClose: func() { close(entered); <-release }}
	f := &Failover{ctx: t.Context(), dial: func(context.Context, string, M.Socksaddr) (net.Conn, error) { return observed, nil }}
	if err := f.Start(adapter.StartStateInitialize, scope); err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = ReadRequest(peer) }()
	conn, err := f.DialContext(t.Context(), N.NetworkTCP, Destination)
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() { _ = scope.Close(); close(closed) }()
	failoverMustFinish(t, entered)
	select {
	case <-closed:
		t.Fatal("serving scope returned before its outbound stream retired")
	case <-time.After(25 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	failoverMustFinish(t, closed)
	if _, err := conn.Write([]byte("retired")); err == nil {
		t.Fatal("retired stream accepted a write")
	}
	if next, err := f.DialContext(t.Context(), N.NetworkTCP, Destination); err == nil {
		_ = next.Close()
		t.Fatal("retired scope admitted a new stream")
	}
}

func TestFailoverCloseInterruptsIOAndReentrantOwner(t *testing.T) {
	for _, restoring := range []bool{false, true} {
		t.Run(map[bool]string{false: "read", true: "restore"}[restoring], func(t *testing.T) {
			raw, peer := net.Pipe()
			defer peer.Close()
			observed := &observedFailoverConn{Conn: raw, read: make(chan struct{})}
			var callbacks atomic.Int32
			var conn *failoverConn
			conn = NewFailoverConn(context.Background(), observed, nil, func() { callbacks.Add(1); _ = conn.Close() })
			if !restoring {
				observed.onClose = func() { _ = conn.Close() }
			}
			readDone := make(chan struct{})
			go func() { _, _ = conn.Read(make([]byte, 1)); close(readDone) }()
			failoverMustFinish(t, observed.read)
			var restoreDone chan struct{}
			if restoring {
				replacement, replacementPeer := net.Pipe()
				defer replacementPeer.Close()
				observed := &observedFailoverConn{Conn: replacement, write: make(chan struct{}), onClose: func() { _ = conn.Close() }}
				restoreDone = make(chan struct{})
				go func() { _ = conn.RestoreConn(observed); close(restoreDone) }()
				failoverMustFinish(t, observed.write)
			}
			closed := make(chan struct{})
			go func() { _ = conn.Close(); close(closed) }()
			failoverMustFinish(t, closed)
			failoverMustFinish(t, readDone)
			if restoreDone != nil {
				failoverMustFinish(t, restoreDone)
			}
			_ = conn.Close()
			if callbacks.Load() != 1 {
				t.Fatal("owner close callback was not exactly once")
			}
		})
	}
}

type admissionFailoverConn struct {
	net.Conn
	*bytes.Reader
}

func (c *admissionFailoverConn) Read(b []byte) (int, error)       { return c.Reader.Read(b) }
func (c *admissionFailoverConn) Write([]byte) (int, error)        { return 0, io.ErrClosedPipe }
func (c *admissionFailoverConn) Close() error                     { return nil }
func (c *admissionFailoverConn) SetDeadline(time.Time) error      { return nil }
func (c *admissionFailoverConn) SetReadDeadline(time.Time) error  { return nil }
func (c *admissionFailoverConn) SetWriteDeadline(time.Time) error { return nil }

func TestFailoverRejectedLegReleasesSessionAdmission(t *testing.T) {
	for _, command := range []byte{255, CommandReconnect} {
		t.Run(map[byte]string{255: "unknown-command", CommandReconnect: "failed-status-write"}[command], func(t *testing.T) {
			id := uuid.Must(uuid.NewV4())
			h := &Inbound{sessionMtx: kmutex.New[uuid.UUID](), conns: map[uuid.UUID]*inboundSession{id: {destination: Destination}}, connections: make(map[*inboundLeg]struct{}), serving: context.Background()}
			router := NewRouter(nil, log.NewNOPFactory().Logger(), h.connHandler)
			var callbacks atomic.Int32
			var request bytes.Buffer
			if err := WriteRequest(&request, &Request{UUID: id, Command: command, Destination: Destination}); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				done := make(chan struct{})
				go func() {
					conn := &admissionFailoverConn{Reader: bytes.NewReader(request.Bytes())}
					router.RouteConnectionEx(context.Background(), conn, adapter.InboundContext{Destination: Destination}, func(error) { callbacks.Add(1) })
					close(done)
				}()
				failoverMustFinish(t, done)
			}
			if callbacks.Load() != 2 {
				t.Fatal("rejected legs repeated or lost their owner callbacks")
			}
		})
	}
}

type scopeFailoverRouter struct {
	adapter.Router
	scope *adapter.Scope
}

func (r *scopeFailoverRouter) RouteConnectionEx(context.Context, net.Conn, adapter.InboundContext, N.CloseHandlerFunc) {
	_ = r.scope.Close()
}

func TestFailoverScopeOwnsPendingAndRoutedLegs(t *testing.T) {
	for _, routed := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "routed-reentrant"}[routed], func(t *testing.T) {
			scope := adapter.NewScope(t.Context(), log.NewNOPFactory().Logger())
			h := &Inbound{Adapter: inbound.NewAdapter("failover", "group"), router: &scopeFailoverRouter{scope: scope}, sessionMtx: kmutex.New[uuid.UUID](), conns: make(map[uuid.UUID]*inboundSession), connections: make(map[*inboundLeg]struct{})}
			if err := h.Start(adapter.StartStateInitialize, scope); err != nil {
				t.Fatal(err)
			}
			raw, peer := net.Pipe()
			defer peer.Close()
			observed := &observedFailoverConn{Conn: raw, read: make(chan struct{})}
			var callbacks atomic.Int32
			done := make(chan struct{})
			go func() {
				_ = h.connHandler(t.Context(), observed, adapter.InboundContext{Destination: Destination}, func(error) { callbacks.Add(1); _ = scope.Close() })
				close(done)
			}()
			failoverMustFinish(t, observed.read)
			if routed {
				if err := WriteRequest(peer, &Request{UUID: uuid.Must(uuid.NewV4()), Command: CommandTCP, Destination: Destination}); err != nil {
					t.Fatal(err)
				}
			} else {
				closed := make(chan struct{})
				go func() { _ = scope.Close(); close(closed) }()
				failoverMustFinish(t, closed)
			}
			failoverMustFinish(t, done)
			if callbacks.Load() != 1 {
				t.Fatal("Scope lost or repeated an accepted-leg callback")
			}
			h.connAccess.Lock()
			remaining := len(h.connections)
			h.connAccess.Unlock()
			h.mtx.RLock()
			sessions := len(h.conns)
			h.mtx.RUnlock()
			if remaining != 0 || sessions != 0 {
				t.Fatal("retired Scope retained native session ownership")
			}
		})
	}
}
