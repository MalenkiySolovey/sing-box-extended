package mieru

import (
	"strings"
	"testing"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
)

func TestMieruOutboundRejectsInvalidOptionsBeforeDialerConstruction(t *testing.T) {
	const privateValue = "private-invalid-mieru-option-canary"
	for _, field := range []string{"transport", "multiplexing", "handshake", "traffic-pattern"} {
		t.Run(field, func(t *testing.T) {
			options := option.MieruOutboundOptions{
				DialerOptions: option.DialerOptions{Detour: "not-constructed"},
				ServerOptions: option.ServerOptions{Server: "127.0.0.1", ServerPort: 443},
				Transport:     "TCP", UserName: "fixture", Password: "synthetic-password",
			}
			switch field {
			case "transport":
				options.Transport = privateValue
			case "multiplexing":
				options.Multiplexing = privateValue
			case "handshake":
				options.HandshakeMode = privateValue
			case "traffic-pattern":
				options.TrafficPattern = privateValue
			}
			outbound, err := NewOutbound(t.Context(), nil, log.NewNOPFactory().Logger(), "invalid", options)
			if outbound != nil || err == nil || strings.Contains(err.Error(), "missing outbound manager") || strings.Contains(err.Error(), privateValue) {
				t.Fatal("invalid Mieru options were not rejected privately before dialer construction")
			}
		})
	}
}
