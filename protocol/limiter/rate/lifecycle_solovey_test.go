package rate

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/service"
)

func TestRateScopeRetiresConfiguredStrategies(t *testing.T) {
	factory := log.NewNOPFactory()
	ctx := service.ContextWith[log.Factory](context.Background(), factory)
	for _, connectionType := range []string{"default", "source_ip"} {
		var options option.RateLimiterOutboundOptions
		if err := json.Unmarshal([]byte(`{"strategy":"users","route":{"final":"direct"},"users":[{"name":"alice","strategy":"token-bucket","connection_type":"`+connectionType+`","count":2,"interval":"1s"}]}`), &options); err != nil {
			t.Fatal(err)
		}
		raw, err := NewOutbound(ctx, nil, factory.Logger(), "rate", options)
		if err != nil {
			t.Fatal(err)
		}
		outbound := raw.(*Outbound)
		strategy := outbound.strategy.(*UsersRateStrategy).strategies["alice"]
		scope := adapter.NewScope(ctx, factory.Logger())
		if err := outbound.Start(adapter.StartStateInitialize, scope); err != nil {
			t.Fatal(err)
		}
		if err := scope.Close(); err != nil {
			t.Fatal(err)
		}
		if err := strategy.request(ctx, &adapter.InboundContext{User: "alice"}); err == nil {
			t.Fatal("closed rate strategy admitted work")
		}
		if err := closeStrategy(strategy); err != nil {
			t.Fatal("repeated strategy retirement", err)
		}
	}
	if _, err := CreateStrategy("token-bucket", "source_ip", -1, 0); err == nil {
		t.Fatal("invalid per-connection rate policy was deferred until first use")
	}
}
