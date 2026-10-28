package v2raykcp

import (
	"testing"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func TestKCPConstructorsRejectUnsafeOptions(t *testing.T) {
	for _, options := range []option.V2RayKCPOptions{
		{TTI: 1001}, {MTU: 1}, {HeaderType: "unknown-header"},
		{ReadBufferSize: 4096}, {UplinkCapacity: ^uint32(0), TTI: 1000},
	} {
		if client, err := NewClient(t.Context(), N.SystemDialer, M.ParseSocksaddr("127.0.0.1:1"), options, nil); err == nil {
			_ = client.Close()
			t.Error("client admitted unsafe native options")
		}
		if server, err := NewServer(t.Context(), log.NewNOPFactory().Logger(), options, nil, servingEcho{}); err == nil {
			_ = server.Close()
			t.Error("server admitted unsafe native options")
		}
	}
}
