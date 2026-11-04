package bandwidth

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/service"
)

func TestUsersBandwidthUsesSelectedUserSpeed(t *testing.T) {
	var options option.BandwidthLimiterOutboundOptions
	if err := json.Unmarshal([]byte(`{"strategy":"users","speed":9000,"route":{"final":"direct"},"users":[{"name":"alice","strategy":"global","mode":"bidirectional","speed":2000},{"name":"bob","strategy":"global","mode":"bidirectional","speed":7000}]}`), &options); err != nil {
		t.Fatal(err)
	}
	factory := log.NewNOPFactory()
	ctx := service.ContextWith[log.Factory](context.Background(), factory)
	raw, err := NewOutbound(ctx, nil, factory.Logger(), "limits", options)
	if err != nil {
		t.Fatal(err)
	}
	users := raw.(*Outbound).strategy.(*UsersBandwidthStrategy)
	for name, want := range map[string]uint64{"alice": 2000, "bob": 7000} {
		selected, err := users.getStrategy(ctx, &adapter.InboundContext{User: name})
		if err != nil {
			t.Fatal(err)
		}
		strategy := selected.(*DefaultWrapStrategy).limiterStrategy.(*GlobalBandwidthStrategy)
		if got := uint64(strategy.limiter.(*speedLimiter).limiter.Limit()); got != want {
			t.Fatalf("%s speed = %d, want %d", name, got, want)
		}
	}
	if _, err := users.getStrategy(ctx, &adapter.InboundContext{User: "absent"}); err == nil {
		t.Fatal("unknown authenticated user acquired another user's policy")
	}
}
