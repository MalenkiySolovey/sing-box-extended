package dion

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kulikov0/headless-client/websocket"
)

type callFixtureTransport func(*http.Request) (*http.Response, error)

func (f callFixtureTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCallDionFailedSetupJoinsSignalingWorker(t *testing.T) {
	subscribed, peerDone := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		connection, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer connection.Close()
		defer close(peerDone)
		if err := connection.WriteJSON(Frame{Method: MethodServerConnected}); err != nil {
			t.Error(err)
			return
		}
		if _, _, err := connection.ReadMessage(); err != nil {
			t.Error(err)
			return
		}
		close(subscribed)
		_, _, _ = connection.ReadMessage()
	}))
	defer server.Close()
	room, _ := json.Marshal(WSSConnectResponse{URL: "ws" + strings.TrimPrefix(server.URL, "http")})
	auth := &Session{AccessToken: "fixture", AccessTokenExp: time.Now().Add(time.Hour), HTTPClient: &http.Client{
		Transport: callFixtureTransport(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(room))}, nil
		}),
	}}
	call := NewCall(CallConfig{Auth: auth, Event: &EventInfo{ID: "fixture"}})
	defer call.Close()
	result := make(chan error, 1)
	go func() { result <- call.Start() }()
	select {
	case <-subscribed:
	case <-time.After(2 * time.Second):
		t.Fatal("native signaling did not enter conference setup")
	}
	call.Close()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("retired conference setup unexpectedly succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("retirement retained conference setup")
	}
	select {
	case <-call.Done():
	default:
		t.Fatal("failed setup returned before joining its signaling worker")
	}
	select {
	case <-peerDone:
	case <-time.After(time.Second):
		t.Fatal("retired signaling retained its socket")
	}
	closed := NewCall(CallConfig{})
	closed.Close()
	if err := closed.Start(); err == nil {
		t.Fatal("closed call admitted a new setup worker")
	}
	closed.closeAndWait()
}
