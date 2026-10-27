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
	"github.com/sagernet/sing-box/common/kmutex"
)

type observedFailoverConn struct {
	net.Conn
	read, write         chan struct{}
	readOnce, writeOnce sync.Once
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

func failoverMustFinish(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("failover lifecycle remained blocked")
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
			readDone := make(chan struct{})
			go func() { _, _ = conn.Read(make([]byte, 1)); close(readDone) }()
			failoverMustFinish(t, observed.read)
			var restoreDone chan struct{}
			if restoring {
				replacement, replacementPeer := net.Pipe()
				defer replacementPeer.Close()
				observed := &observedFailoverConn{Conn: replacement, write: make(chan struct{})}
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
			h := &Inbound{sessionMtx: kmutex.New[uuid.UUID](), conns: map[uuid.UUID]*failoverConn{id: {}}}
			var request bytes.Buffer
			if err := WriteRequest(&request, &Request{UUID: id, Command: command, Destination: Destination}); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				done := make(chan struct{})
				go func() {
					conn := &admissionFailoverConn{Reader: bytes.NewReader(request.Bytes())}
					_ = h.connHandler(context.Background(), conn, adapter.InboundContext{Destination: Destination}, nil)
					close(done)
				}()
				failoverMustFinish(t, done)
			}
		})
	}
}
