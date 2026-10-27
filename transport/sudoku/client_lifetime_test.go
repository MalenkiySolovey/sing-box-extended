package sudoku

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type pendingClientDialer struct {
	entered  chan struct{}
	attempts atomic.Int32
}

type halfCloseClientConn struct {
	net.Conn
	readError, writeError error
	reads, writes, closes int
}

func (c *halfCloseClientConn) CloseRead() error  { c.reads++; return c.readError }
func (c *halfCloseClientConn) CloseWrite() error { c.writes++; return c.writeError }
func (c *halfCloseClientConn) Close() error      { c.closes++; return nil }

func TestSudokuOwnedClientConnPreservesHalfClose(t *testing.T) {
	client, err := NewClient(ClientOptions{Config: ProtocolConfig{Key: "synthetic-client-only", DisableHTTPMask: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	raw := &halfCloseClientConn{readError: errors.New("read-side"), writeError: errors.New("write-side")}
	conn, err := client.own(raw, false)
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(N.CloseWrite(conn), raw.writeError) || raw.writes != 1 || raw.reads != 0 || raw.closes != 0 {
		t.Fatal("owned stream changed write-side closure semantics")
	}
	if !errors.Is(N.CloseRead(conn), raw.readError) || raw.reads != 1 || raw.writes != 1 || raw.closes != 0 {
		t.Fatal("owned stream changed read-side closure semantics")
	}
	client.Close()
	if raw.closes != 1 {
		t.Fatal("half-closed stream escaped owner retirement")
	}
}

func (d *pendingClientDialer) DialContext(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
	if d.attempts.Add(1) == 1 {
		close(d.entered)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*pendingClientDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}

func TestSudokuClientCloseRetiresPendingDialAndAdmission(t *testing.T) {
	dialer := &pendingClientDialer{entered: make(chan struct{})}
	client, err := NewClient(ClientOptions{Dialer: dialer, Config: ProtocolConfig{Key: "synthetic-client-only", ServerAddress: "127.0.0.1:443", DisableHTTPMask: true}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := client.DialContext(ctx, "tcp", M.ParseSocksaddr("192.0.2.1:443")); result <- err }()
	<-dialer.entered
	closed := make(chan struct{})
	go func() { client.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("client Close did not retire its admitted dial")
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("retired pending client dial succeeded")
		}
	case <-time.After(time.Second):
		cancel()
		<-result
		t.Fatal("client Close returned with its admitted dial still pending")
	}
	retired, stop := context.WithCancel(t.Context())
	stop()
	if _, err := client.DialContext(retired, "tcp", M.ParseSocksaddr("192.0.2.1:443")); err == nil || dialer.attempts.Load() != 1 {
		t.Fatal("retired client admitted another network operation")
	}
	if err := client.ResetConnections(); err == nil {
		t.Fatal("retired client accepted a connection reset")
	}
}
