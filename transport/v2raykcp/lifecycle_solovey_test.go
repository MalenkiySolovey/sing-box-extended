package v2raykcp

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type servingEcho struct{}

func (servingEcho) NewConnectionEx(_ context.Context, conn net.Conn, _, _ M.Socksaddr, _ N.CloseHandlerFunc) {
	defer conn.Close()
	_, _ = io.Copy(conn, conn)
}

// An established stream belongs to its transport, rather than the completed
// dial request. Retiring the transport must close that same native stream.
func TestKCPClientServingLifetime(t *testing.T) {
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	options := option.V2RayKCPOptions{Seed: "synthetic-mkcp-fixture", HeaderType: "srtp", TTI: 10}
	server, err := NewServer(t.Context(), log.NewNOPFactory().Logger(), options, nil, servingEcho{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.ServePacket(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("retired server kept its packet reader")
		}
	})
	client, err := NewClient(t.Context(), N.SystemDialer, M.ParseSocksaddr(listener.LocalAddr().String()), options, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	dialCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	conn, err := client.DialContext(dialCtx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	echo := func() error {
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		payload := bytes.Repeat([]byte("native-mkcp-serving-scope/"), 128)
		if _, err := conn.Write(payload); err != nil {
			return err
		}
		got := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, got); err != nil {
			return err
		}
		if !bytes.Equal(got, payload) {
			return io.ErrUnexpectedEOF
		}
		return nil
	}
	if err := echo(); err != nil {
		t.Fatal("native seeded KCP traffic failed", err)
	}
	cancel()
	if err := echo(); err != nil {
		t.Fatal("completed dial cancellation retired a serving stream", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := echo(); err == nil {
		t.Fatal("retired client kept forwarding its native stream")
	}
	if _, err := client.DialContext(t.Context()); err == nil {
		t.Fatal("retired client admitted another dial")
	}
}
