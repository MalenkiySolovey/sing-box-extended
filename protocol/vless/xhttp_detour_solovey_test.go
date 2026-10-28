package vless_test

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/protocol/trojan"
	"github.com/sagernet/sing-box/protocol/vless"
	"github.com/sagernet/sing-box/protocol/vmess"
	sbjson "github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/service"
)

// The download leg is a real outbound dependency even when its canonical
// owner is declared later. Construction must not depend on config row order.
func TestXHTTPDownloadForwardDetourDependency(t *testing.T) {
	for _, kind := range []string{"vless", "vmess", "trojan"} {
		t.Run(kind, func(t *testing.T) {
			registry := outbound.NewRegistry()
			vless.RegisterOutbound(registry)
			vmess.RegisterOutbound(registry)
			trojan.RegisterOutbound(registry)
			manager := outbound.NewManager(registry, endpoint.NewManager(endpoint.NewRegistry()), "")
			ctx := service.ContextWith[adapter.OutboundManager](t.Context(), manager)
			options, _ := registry.CreateOptions(kind)
			fields := map[string]any{"server": "127.0.0.1", "server_port": 443,
				"transport": map[string]any{"type": "xhttp", "mode": "packet-up", "x_padding_bytes": "100-1000",
					"download": map[string]any{"server": "127.0.0.1", "server_port": 443, "x_padding_bytes": "100-1000", "detour": "download-owner"}}}
			if kind == "trojan" {
				fields["password"] = "fixture"
			} else {
				fields["uuid"] = "f0aa8764-88a4-4d8e-8ca8-24df4d750382"
			}
			raw, _ := json.Marshal(fields)
			if err := sbjson.Unmarshal(raw, options); err != nil {
				t.Fatal(err)
			}
			created, err := registry.CreateOutbound(ctx, nil, log.NewNOPFactory().Logger(), "parent", kind, options)
			if err != nil {
				t.Fatal("forward download detour rejected during construction:", err)
			}
			if !slices.Contains(created.Dependencies(), "download-owner") {
				t.Fatal("download detour is absent from native dependency ordering and hot-removal protection")
			}
		})
	}
}
