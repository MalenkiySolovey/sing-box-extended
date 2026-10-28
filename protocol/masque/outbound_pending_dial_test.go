package masque

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	transport "github.com/sagernet/sing-box/transport/masque"
	M "github.com/sagernet/sing/common/metadata"
)

type localMASQUEDialer struct{}

func (localMASQUEDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, destination.String())
}

func (localMASQUEDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return net.ListenPacket("udp", "127.0.0.1:0")
}

func TestMASQUEScopeRetiresPendingL4Handshake(t *testing.T) {
	// Receive a real QUIC Initial without answering it. The native L4 client
	// holds its connection lock throughout this handshake, including Close.
	blackhole, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer blackhole.Close()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := transport.GenerateCert(key, &key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	logger := log.NewNOPFactory().Logger()
	tlsConfig, err := tls.NewMASQUEClient(context.Background(), logger, "localhost", cert, key, &key.PublicKey, option.MASQUEOutboundTLSOptions{Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	created, err := NewOutbound(context.Background(), nil, logger, "parent", option.MASQUEOutboundOptions{UseL4Proxy: true})
	if err != nil {
		t.Fatal(err)
	}
	w := created.(*Outbound)
	w.startHandler = func() {
		defer close(w.await)
		w.client = transport.NewL4Client(logger, transport.L4ClientOptions{
			Dialer: localMASQUEDialer{}, TLSConfig: tlsConfig, Endpoint: blackhole.LocalAddr(),
		})
	}
	scope := adapter.NewScope(context.Background(), logger)
	if err := w.Start(adapter.StartStateInitialize, scope); err != nil {
		t.Fatal(err)
	}
	if err := w.Start(adapter.StartStatePostStart, scope); err != nil {
		t.Fatal(err)
	}
	defer scope.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dialDone := make(chan error, 1)
	go func() {
		conn, err := w.DialContext(ctx, "tcp", M.ParseSocksaddr("192.0.2.1:443"))
		if conn != nil {
			_ = conn.Close()
		}
		dialDone <- err
	}()
	_ = blackhole.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := blackhole.ReadFrom(make([]byte, 2048)); err != nil {
		cancel()
		<-dialDone
		t.Fatal("native QUIC handshake did not start:", err)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- scope.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		cancel()
		<-dialDone
		<-closeDone
		t.Fatal("MASQUE scope waited for an independently owned caller to cancel its pending QUIC handshake")
	}
	select {
	case err := <-dialDone:
		if err == nil {
			t.Fatal("retired MASQUE handshake unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("MASQUE scope returned before joining its pending dial")
	}
}
