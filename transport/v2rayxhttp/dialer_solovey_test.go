package xhttp

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
)

// The inbound scope owns the listener and closes it before the HTTP transport.
// Hold Serve's return so Close deterministically sees its already-closed listener.
func TestXHTTPServerCloseAfterScopeListenerClose(t *testing.T) {
	for _, test := range []struct {
		name     string
		closeErr error
	}{{"already-closed", nil}, {"real-error", errors.New("listener close failure")}} {
		t.Run(test.name, func(t *testing.T) {
			raw, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			listener := &xhttpCloseGateListener{Listener: raw, entered: make(chan struct{}), release: make(chan struct{}), closeErr: test.closeErr}
			server := &Server{httpServer: &http.Server{}}
			finished := make(chan error, 1)
			go func() { finished <- server.Serve(listener) }()
			defer func() {
				_ = raw.Close()
				select {
				case <-listener.release:
				default:
					close(listener.release)
				}
				select {
				case <-finished:
				case <-time.After(5 * time.Second):
					t.Error("HTTP serving did not retire")
				}
			}()
			select {
			case <-listener.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("HTTP listener was not admitted")
			}
			if err := raw.Close(); err != nil {
				t.Fatal(err)
			}
			if err := server.Close(); !errors.Is(err, test.closeErr) {
				t.Fatal("intentional closure must succeed while real close failures remain observable", err)
			}
		})
	}
}

type xhttpCloseGateListener struct {
	net.Listener
	entered, release chan struct{}
	closeErr         error
}

func (l *xhttpCloseGateListener) Accept() (net.Conn, error) {
	close(l.entered)
	conn, err := l.Listener.Accept()
	<-l.release
	return conn, err
}

func (l *xhttpCloseGateListener) Close() error {
	defer close(l.release)
	if l.closeErr != nil {
		return l.closeErr
	}
	return l.Listener.Close()
}

func TestXHTTPOpenStreamPublishesConnectionResultOnce(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(writer, "native-http-stream")
	}))
	defer server.Close()
	client := &DefaultDialerClient{client: server.Client(), options: &option.V2RayXHTTPBaseOptions{}, httpVersion: "1.1"}
	for range 16 {
		stream, remote, local, err := client.OpenStream(t.Context(), server.URL, "owned-session", nil, false)
		if err != nil || remote == nil || local == nil {
			t.Fatal("native HTTP stream lost its connection result", err)
		}
		data, err := io.ReadAll(stream)
		_ = stream.Close()
		if err != nil || string(data) != "native-http-stream" {
			t.Fatal("native HTTP stream failed", err)
		}
	}
	if _, _, _, err := client.OpenStream(t.Context(), "http://127.0.0.1:0", "owned-session", nil, false); err == nil {
		t.Fatal("connection failure was published as a successful dial")
	}
}

func TestXHTTPRetirementKeepsPublishedClientImmutable(t *testing.T) {
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	client := &DefaultDialerClient{client: &http.Client{Transport: transport}}
	client.Close()
	client.Close()
	if !client.IsClosed() {
		t.Fatal("retired XHTTP client remained available")
	}
	// Do reads this field without our mutex. Closing the transport must not
	// rewrite a published http.Client while an admitted POST is still using it.
	if client.client.Transport != transport {
		t.Fatal("retirement mutated the published HTTP client transport")
	}
}
