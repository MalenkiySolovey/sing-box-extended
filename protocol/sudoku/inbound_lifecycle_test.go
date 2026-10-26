package sudoku

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	S "github.com/sagernet/sing-box/transport/sudoku"
	SC "github.com/sagernet/sing-box/transport/sudoku/crypto"
	"github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// A pipe makes admission deterministic: Scope.Close must retire an accepted
// transport even while its first protocol read is waiting for the peer.
type observedReadConn struct {
	net.Conn
	reading chan struct{}
	once    sync.Once
}

func (c *observedReadConn) Read(p []byte) (int, error) {
	c.once.Do(func() { close(c.reading) })
	return c.Conn.Read(p)
}

func TestSudokuScopeRetiresPendingHandshakes(t *testing.T) {
	var options option.SudokuInboundOptions
	if err := json.Unmarshal([]byte(`{"listen":"127.0.0.1","listen_port":0,"key":"sudoku-lifecycle-fixture","disable_http_mask":true,"handshake_timeout":120}`), &options); err != nil {
		t.Fatal(err)
	}
	logger := log.NewNOPFactory().Logger()
	candidate, err := NewInbound(t.Context(), nil, logger, "sudoku-fixture", options)
	if err != nil {
		t.Fatal(err)
	}
	scope := adapter.NewScope(t.Context(), logger)
	t.Cleanup(func() { _ = scope.Close() })
	if err := candidate.Start(adapter.StartStateStart, scope); err != nil {
		t.Fatal(err)
	}
	server, client := net.Pipe()
	observed := &observedReadConn{Conn: server, reading: make(chan struct{})}
	done := make(chan struct{})
	var callbacks atomic.Int32
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("handshake worker did not retire after fixture cleanup")
		}
	})
	go func() {
		candidate.(*Inbound).NewConnection(context.Background(), observed, adapter.InboundContext{}, func(error) { callbacks.Add(1) })
		close(done)
	}()
	select {
	case <-observed.reading:
	case <-time.After(time.Second):
		t.Fatal("accepted handshake did not start reading")
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Scope.Close left an accepted Sudoku handshake running")
	}
	if callbacks.Load() != 1 {
		t.Fatal("accepted handshake did not notify its close handler exactly once")
	}
	_ = client.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Fatal("closed Sudoku transport remained readable")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("closed Sudoku transport retained its peer")
	}
	lateServer, lateClient := net.Pipe()
	t.Cleanup(func() { _ = lateServer.Close(); _ = lateClient.Close() })
	candidate.(*Inbound).NewConnection(t.Context(), lateServer, adapter.InboundContext{}, func(error) { callbacks.Add(1) })
	if callbacks.Load() != 2 {
		t.Fatal("retired inbound admitted a late transport or missed its close handler")
	}
}

type sudokuEchoRouter struct {
	adapter.Router
	observations chan adapter.InboundContext
	workers      sync.WaitGroup
}

func (r *sudokuEchoRouter) RouteConnectionEx(_ context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	r.workers.Add(1)
	r.observations <- metadata
	go func() {
		defer r.workers.Done()
		defer conn.Close()
		_, err := io.Copy(conn, conn)
		if onClose != nil {
			onClose(err)
		}
	}()
}

func (r *sudokuEchoRouter) RoutePacketConnectionEx(_ context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	r.workers.Add(1)
	r.observations <- metadata
	go func() {
		defer r.workers.Done()
		defer conn.Close()
		_, err := bufio.CopyPacket(conn, conn)
		if onClose != nil {
			onClose(err)
		}
	}()
}

func TestSudokuScopeRetiresTrafficAndReusesListener(t *testing.T) {
	for _, tc := range []struct{ mode, mux string }{
		{"raw", "off"}, {"raw", "on"}, {"legacy", "off"},
		{"stream", "off"}, {"poll", "off"}, {"auto", "on"}, {"ws", "on"},
	} {
		t.Run(tc.mode+"/"+tc.mux, func(t *testing.T) {
			reserved, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := reserved.Addr().String()
			port := reserved.Addr().(*net.TCPAddr).Port
			_ = reserved.Close()
			master, err := SC.GenerateMasterKey()
			if err != nil {
				t.Fatal("generate fixture key")
			}
			split, err := SC.SplitPrivateKey(master.Private)
			if err != nil {
				t.Fatal("split fixture key")
			}
			serverMode := tc.mode
			if serverMode == "raw" {
				serverMode = "legacy"
			}
			raw, err := json.Marshal(map[string]any{
				"listen": "127.0.0.1", "listen_port": port,
				"key": SC.EncodePoint(master.Public), "disable_http_mask": tc.mode == "raw",
				"http_mask_mode": serverMode, "path_root": "fixture",
			})
			if err != nil {
				t.Fatal(err)
			}
			var options option.SudokuInboundOptions
			if err := json.Unmarshal(raw, &options); err != nil {
				t.Fatal("decode fixture options")
			}
			router := &sudokuEchoRouter{observations: make(chan adapter.InboundContext, 8)}
			logger := log.NewNOPFactory().Logger()
			candidate, err := NewInbound(t.Context(), router, logger, "sudoku-fixture", options)
			if err != nil {
				t.Fatal("construct Sudoku fixture")
			}
			server := candidate.(*Inbound)
			scope := adapter.NewScope(t.Context(), logger)
			t.Cleanup(func() { _ = scope.Close() })
			if err := candidate.Start(adapter.StartStateStart, scope); err != nil {
				t.Fatal(err)
			}
			cfg := server.protoConf
			cfg.Key = split
			cfg.ServerAddress = address
			cfg.Multiplex = tc.mux
			if tc.mode == "raw" {
				cfg.HTTPMaskMode = "legacy"
			}
			tables, err := S.NewClientTablesWithCustomPatterns(S.ClientAEADSeed(split), "", "", nil)
			if err != nil {
				t.Fatal("construct client tables")
			}
			cfg.Table, cfg.Tables = nil, nil
			if len(tables) == 1 {
				cfg.Table = tables[0]
			} else {
				cfg.Tables = tables
			}
			client, err := S.NewClient(S.ClientOptions{Dialer: &N.DefaultDialer{Dialer: net.Dialer{Timeout: time.Second}}, Config: cfg})
			if err != nil {
				t.Fatal("construct Sudoku client")
			}
			t.Cleanup(client.Close)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			for n := 0; n < 2; n++ {
				conn, err := client.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr("127.0.0.1:9"))
				if err != nil {
					t.Fatal("dial Sudoku TCP fixture", err)
				}
				t.Cleanup(func() { _ = conn.Close() })
				_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
				payload := []byte(fmt.Sprintf("sudoku-scope-data-%d", n))
				if _, err := conn.Write(payload); err != nil {
					t.Fatal("write Sudoku fixture", err)
				}
				got := make([]byte, len(payload))
				if _, err := io.ReadFull(conn, got); err != nil || string(got) != string(payload) {
					t.Fatal("Sudoku TCP payload did not round-trip", err)
				}
				assertSudokuMetadata(t, router, "127.0.0.1:9")
			}
			udp, err := client.DialContext(ctx, N.NetworkUDP, M.ParseSocksaddr("127.0.0.1:53"))
			if err != nil {
				t.Fatal("dial Sudoku UoT fixture", err)
			}
			t.Cleanup(func() { _ = udp.Close() })
			_ = udp.SetDeadline(time.Now().Add(3 * time.Second))
			if err := S.WriteDatagram(udp, "127.0.0.1:53", []byte("sudoku-uot-data")); err != nil {
				t.Fatal("write Sudoku datagram", err)
			}
			destination, payload, err := S.ReadDatagram(udp)
			if err != nil || destination != "127.0.0.1:53" || string(payload) != "sudoku-uot-data" {
				t.Fatal("Sudoku UoT payload did not round-trip", err)
			}
			assertSudokuMetadata(t, router, "")
			closed := make(chan error, 1)
			go func() { closed <- scope.Close() }()
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Sudoku scope did not drain its accepted workers")
			}
			drained := make(chan struct{})
			go func() { router.workers.Wait(); close(drained) }()
			select {
			case <-drained:
			case <-time.After(time.Second):
				t.Fatal("Sudoku routed TCP/UoT sessions survived scope closure")
			}
			server.connAccess.Lock()
			remaining := len(server.connections)
			server.connAccess.Unlock()
			if remaining != 0 {
				t.Fatal("retired Sudoku retained accepted transports")
			}
			client.Close()
			reused, err := net.Listen("tcp", address)
			if err != nil {
				t.Fatal("Sudoku listener port could not be reused", err)
			}
			_ = reused.Close()
		})
	}
}

func assertSudokuMetadata(t *testing.T, router *sudokuEchoRouter, destination string) {
	t.Helper()
	select {
	case metadata := <-router.observations:
		if metadata.Inbound != "sudoku-fixture" || metadata.InboundType != "sudoku" || metadata.User != "" {
			t.Fatal("Sudoku inbound identity was lost or an unauthenticated user identity was invented")
		}
		if destination != "" && metadata.Destination.String() != destination {
			t.Fatal("Sudoku target changed")
		}
	case <-time.After(time.Second):
		t.Fatal("Sudoku routing observation did not arrive")
	}
}

func TestSudokuFailedBindRetiresAdmissionWithoutClosingOccupiedListener(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	raw := fmt.Sprintf(`{"listen":"127.0.0.1","listen_port":%d,"key":"sudoku-lifecycle-fixture","http_mask_mode":"poll"}`, occupied.Addr().(*net.TCPAddr).Port)
	var options option.SudokuInboundOptions
	if err := json.Unmarshal([]byte(raw), &options); err != nil {
		t.Fatal(err)
	}
	logger := log.NewNOPFactory().Logger()
	candidate, err := NewInbound(t.Context(), nil, logger, "failed", options)
	if err != nil {
		t.Fatal(err)
	}
	scope := adapter.NewScope(t.Context(), logger)
	if err := candidate.Start(adapter.StartStateStart, scope); err == nil {
		t.Fatal("occupied Sudoku port was accepted")
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	if err := scope.Close(); err != nil {
		t.Fatal("repeated failed-start cleanup", err)
	}
	if reused, err := net.Listen("tcp", occupied.Addr().String()); err == nil {
		_ = reused.Close()
		t.Fatal("failed Sudoku start closed somebody else's listener")
	}
	server, client := net.Pipe()
	defer client.Close()
	var callbacks atomic.Int32
	candidate.(*Inbound).NewConnection(t.Context(), server, adapter.InboundContext{}, func(error) { callbacks.Add(1) })
	if callbacks.Load() != 1 {
		t.Fatal("failed-start cleanup left admission open")
	}
}

func TestSudokuTrackedTransportPreservesTCPHalfClose(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	client, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	tracked := &inboundConn{Conn: server, inbound: &Inbound{connections: make(map[*inboundConn]struct{})}}
	defer tracked.Close()
	_ = client.SetDeadline(time.Now().Add(time.Second))
	_ = tracked.SetDeadline(time.Now().Add(time.Second))
	if err := tracked.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Read(make([]byte, 1)); err != io.EOF {
		t.Fatal("tracking erased TCP half-close", err)
	}
	if _, err := client.Write([]byte("r")); err != nil {
		t.Fatal("half-close retired the reverse direction", err)
	}
	buffer := make([]byte, 1)
	if _, err := io.ReadFull(tracked, buffer); err != nil || buffer[0] != 'r' {
		t.Fatal("reverse direction did not survive half-close", err)
	}
}
