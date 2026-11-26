package route

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	R "github.com/sagernet/sing-box/route/rule"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func TestStreamRouteGuardPrecedesOutboundDispatch(t *testing.T) {
	target := M.SocksaddrFromNetIP(netip.MustParseAddrPort("192.0.2.1:443"))
	rewrittenIP := netip.MustParseAddr("127.0.0.1")
	rejected := errors.New("target rejected by test guard")
	for _, tc := range []struct {
		name       string
		override   M.Socksaddr
		resolve    bool
		hijack     bool
		tunDNS     bool
		detour     bool
		unguarded  bool
		canceled   bool
		wantErr    error
		wantChecks int
	}{
		{name: "unchanged", wantChecks: 1},
		{name: "address rewrite", override: M.Socksaddr{Addr: rewrittenIP}, wantChecks: 1, wantErr: rejected},
		{name: "port rewrite", override: M.Socksaddr{Port: 22}, wantChecks: 1, wantErr: rejected},
		{name: "resolved addresses", resolve: true, wantChecks: 1, wantErr: rejected},
		{name: "DNS hijack", hijack: true, wantErr: adapter.ErrUnguardedStreamAction},
		{name: "TUN DNS stream", tunDNS: true, wantErr: adapter.ErrUnguardedStreamAction},
		{name: "inbound detour", detour: true, wantErr: adapter.ErrUnguardedStreamAction},
		{name: "canceled", canceled: true, wantErr: context.Canceled},
		{name: "ordinary stream unchanged", override: M.Socksaddr{Port: 22}, unguarded: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			peer, stream := net.Pipe()
			defer peer.Close()
			defer stream.Close()
			egress := &guardTestOutbound{}
			r := &Router{
				ctx: t.Context(), logger: log.NewNOPFactory().Logger(),
				outbound: guardTestOutbounds{selected: egress}, dnsTransport: guardTestTransports{},
				dns:     guardTestDNS{addresses: []netip.Addr{rewrittenIP}},
				started: make(chan struct{}),
			}
			close(r.started)
			metadata := adapter.InboundContext{Inbound: "existing-ingress", Domain: "fixture.invalid", Destination: target}
			if tc.tunDNS {
				metadata.InboundType, metadata.Protocol = C.TypeTun, C.ProtocolDNS
			}
			if tc.detour {
				metadata.InboundDetour = "other-ingress" //nolint:staticcheck
			}
			if tc.resolve {
				metadata.Destination = M.Socksaddr{Fqdn: metadata.Domain, Port: target.Port}
				r.rules = append(r.rules, guardTestRule{action: &R.RuleActionResolve{}})
			}
			var action adapter.RuleAction = &R.RuleActionRoute{Outbound: "selected", RuleActionRouteOptions: R.RuleActionRouteOptions{
				OverrideAddress: M.Socksaddr{Addr: tc.override.Addr}, OverridePort: tc.override.Port,
			}}
			if tc.hijack {
				action = &R.RuleActionHijackDNS{}
			}
			r.rules = append(r.rules, guardTestRule{action: action})
			var checks int
			if !tc.unguarded {
				ctx = adapter.ContextWithStreamRouteGuard(ctx, func(final adapter.InboundContext, outbound adapter.Outbound) error {
					checks++
					if final.Inbound != metadata.Inbound || final.Network != N.NetworkTCP || outbound != egress || egress.called != 0 {
						t.Fatal("guard did not see the selected route before dispatch")
					}
					if tc.resolve && (len(final.DestinationAddresses) != 1 || final.DestinationAddresses[0] != rewrittenIP) {
						t.Fatalf("guard did not see final resolved addresses: %v", final.DestinationAddresses)
					}
					if final.Destination != target || len(final.DestinationAddresses) != 0 {
						return rejected
					}
					return nil
				})
			}
			if tc.canceled {
				cancel()
			}
			if err := r.routeConnection(ctx, stream, metadata, nil); !errors.Is(err, tc.wantErr) {
				t.Fatalf("route error = %v, want %v", err, tc.wantErr)
			}
			if checks != tc.wantChecks || (egress.called == 1) != (tc.wantErr == nil) {
				t.Fatalf("checks=%d dispatches=%d", checks, egress.called)
			}
			if tc.unguarded && egress.destination.Port != 22 {
				t.Fatal("normal routing lost its configured override")
			}
		})
	}
}

type guardTestRule struct {
	adapter.Rule
	action adapter.RuleAction
}

func (r guardTestRule) Match(*adapter.InboundContext) bool { return true }
func (r guardTestRule) Action() adapter.RuleAction         { return r.action }
func (r guardTestRule) String() string                     { return "fixture" }

type guardTestOutbound struct {
	adapter.Outbound
	called      int
	destination M.Socksaddr
}

func (*guardTestOutbound) Network() []string { return []string{N.NetworkTCP} }
func (*guardTestOutbound) Tag() string       { return "selected" }
func (o *guardTestOutbound) NewConnection(_ context.Context, _ net.Conn, m adapter.InboundContext, _ N.CloseHandlerFunc) {
	o.called++
	o.destination = m.Destination
}

type guardTestOutbounds struct {
	adapter.OutboundManager
	selected adapter.Outbound
}

func (m guardTestOutbounds) Outbound(tag string) (adapter.Outbound, bool) {
	return m.selected, tag == m.selected.Tag()
}

type guardTestTransports struct{ adapter.DNSTransportManager }

func (guardTestTransports) FakeIP() adapter.FakeIPTransport { return nil }

type guardTestDNS struct {
	adapter.DNSRouter
	addresses []netip.Addr
}

func (d guardTestDNS) Lookup(context.Context, string, adapter.DNSQueryOptions) ([]netip.Addr, error) {
	return d.addresses, nil
}
