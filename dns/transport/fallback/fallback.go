package fallback

import (
	"context"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/service"

	mDNS "github.com/miekg/dns"
)

func RegisterTransport(registry *dns.TransportRegistry) {
	dns.RegisterTransport[option.FallbackDNSServerOptions](registry, C.DNSTypeFallback, NewTransport)
}

var _ adapter.DNSTransport = (*Transport)(nil)

type Transport struct {
	dns.TransportAdapter
	ctx          context.Context
	manager      adapter.DNSTransportManager
	logger       logger.ContextLogger
	tags         []string
	strategy     ExchangeStrategy
	strategyName string
	timeout      time.Duration
}

func NewTransport(ctx context.Context, logger log.ContextLogger, tag string, options option.FallbackDNSServerOptions) (adapter.DNSTransport, error) {
	if len(options.Servers) == 0 {
		return nil, E.New("missing servers")
	}
	if options.Strategy != "" && options.Strategy != "sequential" && options.Strategy != "parallel" {
		return nil, E.New("invalid fallback DNS strategy")
	}
	if options.Timeout.Build() < 0 {
		return nil, E.New("invalid fallback DNS timeout")
	}
	seen := make(map[string]bool)
	for _, server := range options.Servers {
		if server == "" || server == tag || seen[server] {
			return nil, E.New("invalid fallback DNS server reference")
		}
		seen[server] = true
	}
	manager := service.FromContext[adapter.DNSTransportManager](ctx)
	if manager == nil {
		return nil, E.New("missing DNS transport manager")
	}
	return &Transport{
		TransportAdapter: dns.NewTransportAdapter(C.DNSTypeFallback, tag, options.Servers),
		ctx:              ctx,
		logger:           logger,
		manager:          manager,
		tags:             options.Servers,
		strategyName:     options.Strategy,
		timeout:          options.Timeout.Build(),
	}, nil
}

func (t *Transport) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	// All declarations exist now and the manager has started our dependencies.
	// Constructor-time lookup incorrectly required source-list ordering.
	servers := make([]adapter.DNSTransport, len(t.tags))
	for i, tag := range t.tags {
		server, loaded := t.manager.Transport(tag)
		if !loaded {
			return E.New("fallback DNS dependency not found")
		}
		servers[i] = server
	}
	strategy, err := CreateStrategy(t.strategyName, servers, t.logger, t.timeout)
	if err != nil {
		return err
	}
	t.strategy = strategy
	return nil
}

func (t *Transport) Reset() {
}

func (t *Transport) Exchange(ctx context.Context, message *mDNS.Msg) (*mDNS.Msg, error) {
	if t.strategy == nil {
		return nil, E.New("fallback DNS transport not started")
	}
	return t.strategy(ctx, message)
}

func (t *Transport) ExchangeAsync(ctx context.Context, message *mDNS.Msg, callback func(response *mDNS.Msg, err error)) {
	callback(t.Exchange(ctx, message))
}
