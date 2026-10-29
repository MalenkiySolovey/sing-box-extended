package hysteria2

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"golang.org/x/net/http2"
)

func TestRealmScopeRetiresHTTP2Authority(t *testing.T) {
	logger := log.NewNOPFactory().Logger()
	service, err := NewRealmService(context.Background(), logger, "realm", option.HysteriaRealmServiceOptions{Users: []option.HysteriaRealmUser{{Name: "operator", Token: "realm-scope-fixture", MaxRealms: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	s := service.(*RealmService)
	scope := adapter.NewScope(context.Background(), logger)
	t.Cleanup(func() { _ = scope.Close() })
	for _, stage := range []adapter.StartStage{adapter.StartStateInitialize, adapter.StartStateStart} {
		if err := scope.Start("realm", s, stage); err != nil {
			t.Fatal(err)
		}
	}
	address := s.listener.TCPListener().Addr().String()
	transport := &http2.Transport{AllowHTTP: true, DialTLSContext: func(ctx context.Context, network, address string, _ *tls.Config) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: time.Second}
	request := func(name, token string) (int, error) {
		r, _ := http.NewRequest(http.MethodPost, "http://"+address+"/v1/"+name+"/", strings.NewReader(`{"addresses":["127.0.0.1:12345"]}`))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", "application/json")
		response, err := client.Do(r)
		if err != nil {
			return 0, err
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, response.Body)
		if response.ProtoMajor != 2 {
			t.Fatal("HTTP/2 control connection was not established")
		}
		return response.StatusCode, nil
	}
	if status, err := request("denied", "wrong"); err != nil || status != http.StatusUnauthorized {
		t.Fatal("invalid token admitted", status, err)
	}
	if status, err := request("first", "realm-scope-fixture"); err != nil || status != http.StatusOK {
		t.Fatal("registration failed", status, err)
	}
	if status, err := request("second", "realm-scope-fixture"); err != nil || status != http.StatusTooManyRequests {
		t.Fatal("quota lost", status, err)
	}
	closeErr := scope.Close()
	if _, err := request("retired", "realm-scope-fixture"); err == nil {
		t.Fatal("retired realm still accepts authenticated HTTP/2 requests")
	}
	if closeErr != nil {
		t.Fatal("scope cleanup failed", closeErr)
	}
	if s.ctx.Err() == nil {
		t.Fatal("service context retained")
	}
	s.server.access.Lock()
	remaining := len(s.server.sessions)
	s.server.access.Unlock()
	if remaining != 0 {
		t.Fatal("retired realm retained sessions")
	}
	late := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"addresses":["127.0.0.1:12345"]}`))
	late.Header.Set("Content-Type", "application/json")
	late = late.WithContext(context.WithValue(late.Context(), contextKeyUser, &realmUser{name: "operator"}))
	response := httptest.NewRecorder()
	s.server.handleRegister(response, late)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatal("retired service admitted a late registration")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal("port retained", err)
	}
	_ = listener.Close()
	if err := scope.Close(); err != nil {
		t.Fatal("repeated close failed", err)
	}
}
