package trusttunnel

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"testing"
	"time"
)

type enteredPipeWriter struct {
	*io.PipeWriter
	entered chan struct{}
}

func (w enteredPipeWriter) Write(p []byte) (int, error) {
	close(w.entered)
	return w.PipeWriter.Write(p)
}

func TestTrustTunnelConnectionCloseInterruptsBlockedWrite(t *testing.T) {
	reader, writer := io.Pipe()
	defer reader.Close()
	entered := make(chan struct{})
	conn := &httpConn{writer: enteredPipeWriter{writer, entered}, created: make(chan struct{}), done: make(chan struct{})}
	written := make(chan error, 1)
	go func() { written <- conn.writeChunks([]byte("synthetic-payload")) }()
	<-entered
	closed := make(chan struct{})
	go func() { _ = conn.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("connection Close waited behind a blocked write")
	}
	select {
	case err := <-written:
		if err == nil {
			t.Fatal("closed write succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connection retained its blocked writer")
	}
	_ = conn.Close()
}

type retiringRoundTripper struct{ canceled, release chan struct{} }

func (r retiringRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	defer request.Body.Close()
	if trace := httptrace.ContextClientTrace(request.Context()); trace != nil {
		trace.GotConn(httptrace.GotConnInfo{})
	}
	<-request.Context().Done()
	close(r.canceled)
	<-r.release
	return nil, request.Context().Err()
}

func TestTrustTunnelClientOwnerJoinsWorkersAndRejectsRetiredAdmission(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	canceled, release := make(chan struct{}), make(chan struct{})
	client := &Client{ctx: ctx, cancel: cancel, roundTripper: retiringRoundTripper{canceled, release}, healthCheck: true}
	conn, err := client.Dial(t.Context(), "fixture.invalid:443")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	closed := make(chan struct{})
	go func() { _ = client.Close(); close(closed) }()
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("owner did not cancel its signaling request")
	}
	select {
	case <-closed:
		t.Fatal("owner returned with its request worker still running")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("owner did not join request and health workers")
	}
	if _, err := client.Dial(t.Context(), "fixture.invalid:443"); err != net.ErrClosed {
		t.Fatal("retired client accepted a new stream")
	}
	if _, err := client.ListenPacket(t.Context()); err != net.ErrClosed {
		t.Fatal("retired client accepted a new packet stream")
	}
	mux := &MultiplexClient{ctx: t.Context(), clients: []*Client{client}}
	if err := mux.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := mux.getClient(); err != net.ErrClosed {
		t.Fatal("retired multiplex owner created a new client")
	}
}
