package wtsignal

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

type ownedSignalPacket struct {
	net.PacketConn
	closed chan struct{}
	once   sync.Once
}

func (p *ownedSignalPacket) Close() error {
	p.once.Do(func() { close(p.closed) })
	return p.PacketConn.Close()
}

type signalDialer struct{ packet *ownedSignalPacket }

func (*signalDialer) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, errors.New("unexpected stream dial")
}

func (d *signalDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return d.packet, nil
}

func TestCallWebTransportUsesOwnedPacketAndCancelsHandshake(t *testing.T) {
	server, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	packet := &ownedSignalPacket{PacketConn: client, closed: make(chan struct{})}
	defer packet.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := DialContext(ctx, &signalDialer{packet}, "https://"+server.LocalAddr().String()+"/wt", "localhost", "127.0.0.1", "https://example.invalid")
		done <- err
	}()
	_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
	buffer := make([]byte, 2048)
	if _, _, err := server.ReadFrom(buffer); err != nil {
		t.Fatal("native packet dialer did not carry the QUIC handshake")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("silent WebTransport peer unexpectedly connected")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WebTransport handshake outlived serving cancellation")
	}
	select {
	case <-packet.closed:
	case <-time.After(time.Second):
		t.Fatal("WebTransport leaked its native packet transport")
	}
}
