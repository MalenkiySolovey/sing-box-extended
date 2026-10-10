package ssmapi

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/listener"
	"github.com/sagernet/sing-box/option"
)

func TestScopeClosesStartedSSMAndReleasesListener(t *testing.T) {
	s := cacheServiceFixture()
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.listener = listener.New(listener.Options{Context: s.ctx, Logger: s.logger, Listen: option.ListenOptions{}})
	s.httpServer = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })}
	scope := adapter.NewScope(context.Background(), s.logger)
	t.Cleanup(func() { _ = scope.Close() })
	if err := scope.Start("ssm-test", s, adapter.StartStateStart); err != nil {
		t.Fatal(err)
	}
	address := s.listener.TCPListener().Addr().String()
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	response, err := client.Get("http://" + address)
	if err != nil {
		t.Fatal("HTTP readiness", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatal("HTTP readiness rejected")
	}
	if err = scope.Close(); err != nil {
		t.Fatal("scope cleanup", err)
	}
	reused, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal("scope retained listener", err)
	}
	defer reused.Close()
	if !errors.Is(s.ctx.Err(), context.Canceled) {
		t.Fatal("scope retained service context")
	}
	if err = scope.Close(); err != nil {
		t.Fatal("repeated scope cleanup", err)
	}
}
