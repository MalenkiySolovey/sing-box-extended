package bond

import (
	"bytes"
	"context"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"
)

// Encode independently of WriteRequest so malformed peer input remains testable
// even when local writers validate their own requests.
func requestWire(t *testing.T, request Request) []byte {
	t.Helper()
	var wire bytes.Buffer
	wire.WriteByte(Version)
	wire.Write(request.UUID[:])
	wire.Write([]byte{request.Index, request.Count, request.DownloadRatio, request.UploadRatio})
	if err := AddressSerializer.WriteAddrPort(&wire, request.Destination); err != nil {
		t.Fatal(err)
	}
	return wire.Bytes()
}

func TestBondRejectsMalformedLegHeaders(t *testing.T) {
	valid := Request{Count: 2, DownloadRatio: 50, UploadRatio: 50, Destination: M.ParseSocksaddr("example.test:443")}
	for name, mutate := range map[string]func(*Request){
		"zero-count":         func(r *Request) { r.Count = 0 },
		"index-equals-count": func(r *Request) { r.Index = r.Count },
		"index-over-count":   func(r *Request) { r.Index = 255 },
		"download-over-100":  func(r *Request) { r.DownloadRatio = 101 },
		"upload-over-100":    func(r *Request) { r.UploadRatio = 255 },
	} {
		t.Run(name, func(t *testing.T) {
			request := valid
			mutate(&request)
			if _, err := ReadRequest(bytes.NewReader(requestWire(t, request))); err == nil {
				t.Fatal("accepted malformed Bond leg header")
			}
		})
	}
	// A leg may intentionally carry traffic in only one direction.
	valid.Index, valid.Count, valid.DownloadRatio, valid.UploadRatio = 254, 255, 0, 100
	if _, err := ReadRequest(bytes.NewReader(requestWire(t, valid))); err != nil {
		t.Fatal("rejected valid one-direction leg:", err)
	}
}

func TestBondOutboundRejectsOverflowBeforeConstructingChildren(t *testing.T) {
	for name, legs := range map[string][]option.BondOutbound{
		"ratio-byte-overflow": {{Count: 2, DownloadRatio: 178, UploadRatio: 178}},
		"count-byte-overflow": {{Count: 255}, {DownloadRatio: 100, UploadRatio: 100}},
		"empty":               nil,
	} {
		t.Run(name, func(t *testing.T) {
			registry := outbound.NewRegistry()
			created := 0
			outbound.Register[struct{}](registry, "fixture", func(context.Context, adapter.Router, log.ContextLogger, string, struct{}) (adapter.Outbound, error) {
				created++
				return &struct{ adapter.Outbound }{}, nil
			})
			for i := range legs {
				legs[i].Outbound.Type = "fixture"
			}
			ctx := service.ContextWith[adapter.OutboundRegistry](t.Context(), registry)
			if _, err := NewOutbound(ctx, nil, log.NewNOPFactory().Logger(), "bond-fixture", option.BondOutboundOptions{Outbounds: legs}); err == nil {
				t.Error("accepted invalid Bond leg totals")
			}
			if created != 0 {
				t.Fatal("invalid candidate constructed child outbounds before validation")
			}
		})
	}
}
