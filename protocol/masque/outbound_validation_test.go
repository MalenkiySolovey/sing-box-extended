package masque

import (
	"strings"
	"testing"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
)

func TestMASQUERejectsInvalidOptionsBeforeProfileBootstrap(t *testing.T) {
	for name, options := range map[string]option.MASQUEOutboundOptions{
		"endpoint":     {Address: "private-invalid-endpoint-canary"},
		"congestion":   {CongestionController: "private-invalid-congestion-canary"},
		"profile-pair": {Profile: option.CloudflareProfile{ID: "private-profile-id-canary"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewOutbound(t.Context(), nil, log.NewNOPFactory().Logger(), "candidate", options)
			if err == nil {
				t.Fatal("invalid candidate reached asynchronous profile bootstrap")
			}
			if strings.Contains(err.Error(), "canary") {
				t.Fatal("native validation disclosed option material")
			}
		})
	}
}
