package xhttp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sagernet/sing-box/option"
)

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
