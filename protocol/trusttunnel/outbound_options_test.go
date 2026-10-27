package trusttunnel

import (
	"strings"
	"testing"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
)

func TestTrustTunnelOutboundValidatesRequiredOptionsBeforeDialerConstruction(t *testing.T) {
	for _, field := range []string{"server", "port", "username", "password", "multiplex"} {
		t.Run(field, func(t *testing.T) {
			options := option.TrustTunnelOutboundOptions{
				DialerOptions:               option.DialerOptions{Detour: "not-constructed"},
				ServerOptions:               option.ServerOptions{Server: "127.0.0.1", ServerPort: 443},
				OutboundTLSOptionsContainer: option.OutboundTLSOptionsContainer{TLS: &option.OutboundTLSOptions{Enabled: true}},
				Username:                    "fixture", Password: "synthetic-password",
			}
			switch field {
			case "server":
				options.Server = ""
			case "port":
				options.ServerPort = 0
			case "username":
				options.Username = ""
			case "password":
				options.Password = ""
			case "multiplex":
				options.Multiplex = &option.TrustTunnelMultiplexOptions{MaxConnections: -1}
			}
			outbound, err := NewOutbound(t.Context(), nil, log.NewNOPFactory().Logger(), "invalid", options)
			if outbound != nil || err == nil || strings.Contains(err.Error(), "missing outbound manager") {
				t.Fatal("invalid TrustTunnel candidate reached dialer construction")
			}
		})
	}
}
