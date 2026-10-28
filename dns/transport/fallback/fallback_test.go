package fallback

import (
	"context"
	"errors"
	"testing"
	"time"

	mDNS "github.com/miekg/dns"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
	"github.com/sagernet/sing/service"
)

type fixtureManager struct {
	adapter.DNSTransportManager
	transports map[string]adapter.DNSTransport
}

func (m *fixtureManager) Transport(tag string) (adapter.DNSTransport, bool) {
	server, ok := m.transports[tag]
	return server, ok
}

type fixtureTransport struct {
	adapter.DNSTransport
	failed bool
}

func (s fixtureTransport) Exchange(_ context.Context, request *mDNS.Msg) (*mDNS.Msg, error) {
	if s.failed {
		return nil, errors.New("fixture unavailable")
	}
	response := new(mDNS.Msg)
	response.SetReply(request)
	return response, nil
}
func (s fixtureTransport) Tag() string { return "fixture" }

func TestFallbackResolvesDependenciesAfterDeclaration(t *testing.T) {
	for _, strategy := range []string{"", "sequential", "parallel"} {
		t.Run(strategy, func(t *testing.T) {
			manager := &fixtureManager{transports: map[string]adapter.DNSTransport{}}
			ctx := service.ContextWith[adapter.DNSTransportManager](context.Background(), manager)
			logger := log.NewNOPFactory().NewLogger("fixture")
			raw, err := NewTransport(ctx, logger, "fallback", option.FallbackDNSServerOptions{Servers: []string{"later-a", "later-b"}, Strategy: strategy})
			if err != nil {
				t.Fatalf("later declaration rejected by constructor: %v", err)
			}
			transport := raw.(*Transport)
			if len(transport.Dependencies()) != 2 || transport.Tag() != "fallback" {
				t.Fatal("fallback identity/dependencies missing")
			}
			request := new(mDNS.Msg)
			request.SetQuestion("example.invalid.", mDNS.TypeA)
			if _, err := transport.Exchange(ctx, request); err == nil {
				t.Fatal("unstarted transport accepted query")
			}
			manager.transports["later-a"] = fixtureTransport{failed: true}
			manager.transports["later-b"] = fixtureTransport{}
			scope := adapter.NewScope(ctx, logger)
			defer scope.Close()
			if err := transport.Start(adapter.StartStateStart, scope); err != nil {
				t.Fatal(err)
			}
			bounded, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			response, err := transport.Exchange(bounded, request)
			if err != nil || response == nil || response.Rcode != mDNS.RcodeSuccess {
				t.Fatalf("fallback query failed: %v", err)
			}
		})
	}
}

func TestFallbackRejectsInvalidOptionsBeforeStart(t *testing.T) {
	for _, options := range []option.FallbackDNSServerOptions{
		{},
		{Servers: []string{""}},
		{Servers: []string{"fallback"}},
		{Servers: []string{"a", "a"}},
		{Servers: []string{"a"}, Strategy: "unknown"},
		{Servers: []string{"a"}, Timeout: badoption.Duration(-time.Second)},
	} {
		if _, err := NewTransport(context.Background(), nil, "fallback", options); err == nil {
			t.Fatal("invalid fallback options reached runtime")
		}
	}
}
