package httpmask

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestTunnelServerCloseRetiresDetachedSessionAndPull(t *testing.T) {
	server := NewTunnelServer(TunnelServerOptions{Mode: "poll", SessionTTL: time.Hour})
	t.Cleanup(func() { _ = server.Close() })
	raw, client := net.Pipe()
	t.Cleanup(func() { _ = raw.Close(); _ = client.Close() })
	logical := make(chan net.Conn, 1)
	go func() {
		_, conn, _ := server.HandleConn(raw)
		logical <- conn
	}()
	_ = client.SetDeadline(time.Now().Add(time.Second))
	if _, err := io.WriteString(client, "GET /session HTTP/1.1\r\nHost: fixture\r\nX-Sudoku-Tunnel: poll\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(client), nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	authorization, err := parseAuthorizeResponse(body)
	if err != nil {
		t.Fatal(err)
	}
	var conn net.Conn
	select {
	case conn = <-logical:
	case <-time.After(time.Second):
		t.Fatal("session authorization did not finish")
	}
	if conn == nil {
		t.Fatal("session did not return its logical transport")
	}
	t.Cleanup(func() { _ = conn.Close() })
	sess, ok := server.sessionGet(authorization.token)
	if !ok {
		t.Fatal("authorized session missing")
	}
	pullRaw, pullPeer := net.Pipe()
	t.Cleanup(func() { _ = pullRaw.Close(); _ = pullPeer.Close() })
	lease, ok := server.beginSessionPull(authorization.token, sess, pullRaw)
	if !ok {
		t.Fatal("pull lease was not admitted")
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-lease.cancel:
	default:
		t.Fatal("server close retained the active HTTP pull lease")
	}
	if server.sessionHas(authorization.token) {
		t.Fatal("server close retained the detached logical session")
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("server close left its logical pipe readable")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("server close did not retire the logical pipe")
	}
	if _, ok := server.beginSessionPull(authorization.token, sess, pullRaw); ok {
		t.Fatal("closed session admitted a new pull")
	}
	lateRaw, latePeer := net.Pipe()
	defer latePeer.Close()
	if _, _, err := server.HandleConn(lateRaw); !errors.Is(err, net.ErrClosed) {
		t.Fatal("closed server accepted a new request")
	}
	var closes sync.WaitGroup
	for range 4 {
		closes.Add(1)
		go func() { defer closes.Done(); _ = server.Close() }()
	}
	closes.Wait()
}

func TestTunnelServerCloseRacingSessionAuthorization(t *testing.T) {
	preparing, release := make(chan struct{}), make(chan struct{})
	var logical net.Conn
	server := NewTunnelServer(TunnelServerOptions{Mode: "poll", EarlyHandshake: &TunnelServerEarlyHandshake{
		Prepare: func([]byte) (*PreparedServerEarlyHandshake, error) {
			close(preparing)
			<-release
			return &PreparedServerEarlyHandshake{WrapConn: func(conn net.Conn) (net.Conn, error) { logical = conn; return conn, nil }}, nil
		},
	}})
	raw, peer := net.Pipe()
	defer peer.Close()
	done := make(chan error, 1)
	go func() { _, _, err := server.sessionAuthorize(raw, nil, nil, []byte("fixture")); done <- err }()
	<-preparing
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatal("late session was published after close", err)
		}
	case <-time.After(time.Second):
		t.Fatal("late authorization did not finish")
	}
	if logical == nil {
		t.Fatal("early handshake did not reach the admission race")
	}
	_ = logical.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := logical.Read(make([]byte, 1)); err == nil {
		t.Fatal("rejected early-handshake pipe remained open")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("rejected early-handshake pipe was not closed")
	}
	if len(server.sessions) != 0 {
		t.Fatal("late authorization left a session or TTL worker")
	}
}
