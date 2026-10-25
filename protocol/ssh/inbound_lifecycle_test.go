package ssh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	N "github.com/sagernet/sing/common/network"
	sshlib "golang.org/x/crypto/ssh"
)

type sshEchoRouter struct {
	adapter.Router
	observations chan adapter.InboundContext
}

func (r *sshEchoRouter) RouteConnectionEx(_ context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	r.observations <- metadata
	go func() {
		defer conn.Close()
		_, err := io.Copy(conn, conn)
		if onClose != nil {
			onClose(err)
		}
	}()
}

func sshOptions(t *testing.T, port int, users string) option.SSHInboundOptions {
	t.Helper()
	var options option.SSHInboundOptions
	raw := fmt.Sprintf(`{"listen":"127.0.0.1","listen_port":%d%s}`, port, users)
	if err := json.Unmarshal([]byte(raw), &options); err != nil {
		t.Fatal(err)
	}
	return options
}

func reserveSSHPort(t *testing.T) (net.Listener, int) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener, listener.Addr().(*net.TCPAddr).Port
}

func TestSSHEmptyUsersKeepListenerInactive(t *testing.T) {
	for _, users := range []string{"", `,"users":[]`} {
		t.Run(fmt.Sprintf("users=%s", users), func(t *testing.T) {
			occupied, port := reserveSSHPort(t)
			logger := log.NewNOPFactory().Logger()
			candidate, err := NewInbound(t.Context(), nil, logger, "idle", sshOptions(t, port, users))
			if err != nil {
				t.Fatal("empty SSH user list was rejected", err)
			}
			scope := adapter.NewScope(t.Context(), logger)
			t.Cleanup(func() { _ = scope.Close() })
			if err := candidate.Start(adapter.StartStateStart, scope); err != nil {
				t.Fatal("inactive SSH inbound tried to bind the occupied port", err)
			}
			_ = occupied.Close()
			probe, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
			if err != nil {
				t.Fatal("inactive SSH inbound retained a listener", err)
			}
			_ = probe.Close()
			if err := scope.Close(); err != nil {
				t.Fatal(err)
			}
			if err := scope.Close(); err != nil {
				t.Fatal("repeated inactive close failed", err)
			}
		})
	}
	options := sshOptions(t, 12345, `,"users":[]`)
	options.HostKey = []string{"invalid-private-key"}
	if _, err := NewInbound(t.Context(), nil, log.NewNOPFactory().Logger(), "invalid", options); err == nil {
		t.Fatal("empty users bypassed host-key validation")
	}
}

func startSSH(t *testing.T) (*Inbound, *adapter.Scope, *sshEchoRouter, string) {
	t.Helper()
	reserved, port := reserveSSHPort(t)
	_ = reserved.Close()
	router := &sshEchoRouter{observations: make(chan adapter.InboundContext, 16)}
	logger := log.NewNOPFactory().Logger()
	candidate, err := NewInbound(t.Context(), router, logger, "ssh-fixture", sshOptions(t, port, `,"users":[{"name":"alice","password":"synthetic-alice"},{"name":"bob","password":"synthetic-bob"}]`))
	if err != nil {
		t.Fatal(err)
	}
	scope := adapter.NewScope(t.Context(), logger)
	t.Cleanup(func() { _ = scope.Close() })
	if err := candidate.Start(adapter.StartStateStart, scope); err != nil {
		t.Fatal(err)
	}
	return candidate.(*Inbound), scope, router, fmt.Sprintf("127.0.0.1:%d", port)
}

func sshClient(t *testing.T, address, user string) *sshlib.Client {
	t.Helper()
	client, err := sshlib.Dial("tcp", address, &sshlib.ClientConfig{
		User: user, Auth: []sshlib.AuthMethod{sshlib.Password("synthetic-" + user)},
		HostKeyCallback: sshlib.InsecureIgnoreHostKey(), Timeout: time.Second,
	})
	if err != nil {
		t.Fatal("SSH authentication failed", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func sshRoundtrip(t *testing.T, client *sshlib.Client) {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		conn, err := client.Dial("tcp", "127.0.0.1:9")
		if err != nil {
			result <- err
			return
		}
		defer conn.Close()
		payload := []byte("authenticated-ssh-data-plane")
		if _, err := conn.Write(payload); err != nil {
			result <- err
			return
		}
		read := make([]byte, len(payload))
		_, err = io.ReadFull(conn, read)
		if err == nil && string(read) != string(payload) {
			err = fmt.Errorf("SSH payload changed")
		}
		result <- err
	}()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SSH payload did not complete")
	}
}

func TestSSHScopeRetiresAuthenticatedSessionsAndPendingHandshakes(t *testing.T) {
	server, scope, router, address := startSSH(t)
	clients := make([]*sshlib.Client, 0, 2)
	for _, user := range []string{"alice", "bob"} {
		client := sshClient(t, address, user)
		clients = append(clients, client)
		sshRoundtrip(t, client)
		metadata := <-router.observations
		if metadata.User != user || metadata.Inbound != "ssh-fixture" || metadata.InboundType != "ssh" {
			t.Fatal("authenticated SSH identity was not retained")
		}
	}
	stalled, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stalled.Close() })
	deadline := time.Now().Add(time.Second)
	for {
		server.connAccess.Lock()
		count := len(server.connections)
		server.connAccess.Unlock()
		if count == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pending SSH handshake was not admitted")
		}
		time.Sleep(time.Millisecond)
	}
	closed := make(chan error, 1)
	go func() { closed <- scope.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("SSH scope did not drain accepted sessions and pending handshakes")
	}
	server.connAccess.Lock()
	retained := len(server.connections)
	server.connAccess.Unlock()
	if retained != 0 {
		t.Fatal("SSH scope retained an accepted connection")
	}
	for _, client := range clients {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		conn, err := client.DialContext(ctx, "tcp", "127.0.0.1:9")
		cancel()
		if conn != nil {
			_ = conn.Close()
		}
		if err == nil {
			t.Fatal("retired authenticated SSH session opened another forwarding channel")
		}
	}
	if err := scope.Close(); err != nil {
		t.Fatal("repeated SSH close failed", err)
	}
	probe, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal("closed SSH scope retained its listening port", err)
	}
	_ = probe.Close()
}

func TestSSHClosedScopeRejectsInjectedConnections(t *testing.T) {
	server, scope, _, _ := startSSH(t)
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	serverConn, peer := net.Pipe()
	defer peer.Close()
	var callbacks atomic.Int32
	server.NewConnection(t.Context(), serverConn, adapter.InboundContext{}, func(error) { callbacks.Add(1) })
	if callbacks.Load() != 1 {
		t.Fatal("retired SSH injection did not close exactly once")
	}
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := peer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
		t.Fatal("retired SSH injection retained its peer")
	}
}

func TestSSHScopeRetiresExplicitFallback(t *testing.T) {
	for _, stalledHandshake := range []bool{true, false} {
		t.Run(fmt.Sprintf("stalled-handshake=%t", stalledHandshake), func(t *testing.T) {
			var fallbackAddress string
			accepted := make(chan net.Conn, 1)
			if stalledHandshake {
				upstream, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer upstream.Close()
				fallbackAddress = upstream.Addr().String()
				go func() {
					conn, err := upstream.Accept()
					if err == nil {
						accepted <- conn
					}
				}()
			} else {
				_, _, _, fallbackAddress = startSSH(t)
			}
			reserved, port := reserveSSHPort(t)
			_ = reserved.Close()
			fallbackHost, fallbackPortText, err := net.SplitHostPort(fallbackAddress)
			if err != nil {
				t.Fatal(err)
			}
			options := sshOptions(t, port, fmt.Sprintf(`,"fallback":{"server":%q,"server_port":%s}`, fallbackHost, fallbackPortText))
			logger := log.NewNOPFactory().Logger()
			candidate, err := NewInbound(t.Context(), nil, logger, "fallback-fixture", options)
			if err != nil {
				t.Fatal(err)
			}
			scope := adapter.NewScope(t.Context(), logger)
			t.Cleanup(func() { _ = scope.Close() })
			if err := candidate.Start(adapter.StartStateStart, scope); err != nil {
				t.Fatal(err)
			}
			address := fmt.Sprintf("127.0.0.1:%d", port)
			var authenticationDone chan error
			var client *sshlib.Client
			var flow net.Conn
			if stalledHandshake {
				authenticationDone = make(chan error, 1)
				go func() {
					client, err := sshlib.Dial("tcp", address, &sshlib.ClientConfig{
						User: "alice", Auth: []sshlib.AuthMethod{sshlib.Password("synthetic-alice")},
						HostKeyCallback: sshlib.InsecureIgnoreHostKey(), Timeout: time.Second,
					})
					if client != nil {
						_ = client.Close()
					}
					authenticationDone <- err
				}()
				select {
				case upstream := <-accepted:
					defer upstream.Close()
				case <-time.After(time.Second):
					t.Fatal("SSH fallback did not reach its private handshake fixture")
				}
			} else {
				client = sshClient(t, address, "alice")
				sshRoundtrip(t, client)
				flow, err = client.Dial("tcp", "127.0.0.1:9")
				if err != nil {
					t.Fatal(err)
				}
				defer flow.Close()
			}
			closed := make(chan error, 1)
			go func() { closed <- scope.Close() }()
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("SSH scope retained an explicit fallback handshake or forwarding channel")
			}
			if stalledHandshake {
				select {
				case err := <-authenticationDone:
					if err == nil {
						t.Fatal("retired SSH fallback authenticated")
					}
				case <-time.After(time.Second):
					t.Fatal("retired SSH fallback retained the authenticating client")
				}
			} else {
				_ = flow.SetReadDeadline(time.Now().Add(time.Second))
				if _, err := flow.Read(make([]byte, 1)); err == nil || os.IsTimeout(err) {
					t.Fatal("retired SSH fallback retained a forwarding channel")
				}
			}
		})
	}
}
