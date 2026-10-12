package v2raywebsocket

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type pathEcho struct {
	payload  []byte
	done     chan error
	accepted atomic.Bool
}

func (h *pathEcho) NewConnectionEx(_ context.Context, conn net.Conn, _, _ M.Socksaddr, _ N.CloseHandlerFunc) {
	h.accepted.Store(true)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	buffer := make([]byte, len(h.payload))
	_, err := io.ReadFull(conn, buffer)
	if err == nil && !bytes.Equal(buffer, h.payload) {
		err = io.ErrUnexpectedEOF
	}
	if err == nil {
		_, err = conn.Write(buffer)
	}
	h.done <- err
}

func (*pathEcho) NewPacketConnectionEx(_ context.Context, conn N.PacketConn, _, _ M.Socksaddr, _ N.CloseHandlerFunc) {
	_ = conn.Close()
}

func TestLiteralPathEarlyDataModes(t *testing.T) {
	for _, path := range []string{"/fixture?token=a%26b", "/fixture%2Fsegment"} {
		for _, mode := range []struct {
			name   string
			max    uint32
			header string
		}{{"disabled", 0, ""}, {"path", 32, ""}, {"header", 32, "Sec-WebSocket-Protocol"}} {
			t.Run(path+"/"+mode.name, func(t *testing.T) {
				h := &pathEcho{payload: []byte("authenticated-protocol-independent-path-nonce"), done: make(chan error, 1)}
				logger := log.NewNOPFactory().Logger()
				server, err := NewServer(t.Context(), logger, option.V2RayWebsocketOptions{Path: path, MaxEarlyData: mode.max, EarlyDataHeaderName: mode.header}, nil, h)
				if err != nil {
					t.Fatal(err)
				}
				local := httptest.NewServer(server)
				defer local.Close()
				client, err := NewClient(t.Context(), N.SystemDialer, M.ParseSocksaddr(local.Listener.Addr().String()), option.V2RayWebsocketOptions{Path: (&url.URL{Path: path}).EscapedPath(), MaxEarlyData: mode.max, EarlyDataHeaderName: mode.header}, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
				defer cancel()
				conn, err := client.DialContext(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
				if _, err = conn.Write(h.payload); err != nil {
					t.Fatal(err)
				}
				response := make([]byte, len(h.payload))
				if _, err = io.ReadFull(conn, response); err != nil || !bytes.Equal(response, h.payload) {
					t.Fatal("literal path nonce failed", err)
				}
				select {
				case err := <-h.done:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("transport handler did not finish")
				}
			})
		}
	}
}

func TestOversizedEarlyDataRejectedBeforeHandler(t *testing.T) {
	h := &pathEcho{payload: []byte("too-large-for-configured-two-bytes"), done: make(chan error, 1)}
	logger := log.NewNOPFactory().Logger()
	server, err := NewServer(t.Context(), logger, option.V2RayWebsocketOptions{Path: "/fixture", MaxEarlyData: 2, EarlyDataHeaderName: "Sec-WebSocket-Protocol"}, nil, h)
	if err != nil {
		t.Fatal(err)
	}
	local := httptest.NewServer(server)
	defer local.Close()
	client, err := NewClient(t.Context(), N.SystemDialer, M.ParseSocksaddr(local.Listener.Addr().String()), option.V2RayWebsocketOptions{Path: "/fixture", MaxEarlyData: 64, EarlyDataHeaderName: "Sec-WebSocket-Protocol"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	conn, err := client.DialContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	_, err = conn.Write(h.payload)
	if err == nil {
		buffer := make([]byte, 1)
		_, err = conn.Read(buffer)
	}
	if err == nil || h.accepted.Load() {
		t.Fatal("oversized early data reached transport handler")
	}
}
