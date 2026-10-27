package mtproxy

import (
	"context"
	"net"
	"strings"
	"sync"

	"github.com/dolonet/mtg-multi/antireplay"
	"github.com/dolonet/mtg-multi/events"
	"github.com/dolonet/mtg-multi/mtglib"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/common/listener"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	N "github.com/sagernet/sing/common/network"
)

func RegisterInbound(registry *inbound.Registry) {
	inbound.Register[option.MTProxyInboundOptions](registry, C.TypeMTProxy, NewInbound)
}

type Inbound struct {
	inbound.Adapter
	ctx      context.Context
	router   adapter.ConnectionRouterEx
	logger   logger.ContextLogger
	listener *listener.Listener
	proxy    *mtglib.Proxy
	options  mtglib.ProxyOpts
	access   sync.Mutex
	routing  sync.RWMutex
	retired  bool
}

func NewInbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.MTProxyInboundOptions) (adapter.Inbound, error) {
	inbound := &Inbound{
		Adapter: inbound.NewAdapter(C.TypeMTProxy, tag),
		ctx:     ctx,
		router:  router,
		logger:  logger,
		listener: listener.New(listener.Options{
			Context: ctx,
			Logger:  logger,
			Listen:  options.ListenOptions,
		}),
	}
	mtgLogger := NewLoggerAdapter(logger)
	secrets := make(map[string]mtglib.Secret, len(options.Users))
	for _, user := range options.Users {
		secret := mtglib.Secret{}
		if err := secret.Set(user.Secret); err != nil || !secret.Valid() {
			return nil, E.New("invalid mtproxy user secret")
		}
		secrets[user.Name] = secret
	}
	opts := mtglib.ProxyOpts{
		Logger:          mtgLogger,
		Network:         NewNetworkAdapter(ctx, NewDialer(inbound.newConnection)),
		AntiReplayCache: antireplay.NewNoop(),
		EventStream:     events.NewNoopStream(),

		Secrets:                     secrets,
		Concurrency:                 options.GetConcurrency(),
		DomainFrontingPort:          options.GetDomainFrontingPort(),
		DomainFrontingHost:          options.DomainFrontingHost,
		DomainFrontingProxyProtocol: options.DomainFrontingProxyProtocol,
		PreferIP:                    options.GetPreferIP(),
		AutoUpdate:                  options.AutoUpdate,

		AllowFallbackOnUnknownDC: options.AllowFallbackOnUnknownDC,
		TolerateTimeSkewness:     options.TolerateTimeSkewness.Build(),
		IdleTimeout:              options.GetIdleTimeout(),
		HandshakeTimeout:         options.GetHandshakeTimeout(),

		DoppelGangerURLs:    options.DoppelGangerURLs,
		DoppelGangerPerRaid: options.GetDoppelGangerPerRaid(),
		DoppelGangerEach:    options.GetDoppelGangerEach(),
		DoppelGangerDRS:     options.DoppelGangerDRS,

		ThrottleMaxConnections: options.ThrottleMaxConnections,
		ThrottleCheckInterval:  options.GetThrottleCheckInterval(),
	}
	// NewProxy starts MTG background work. Validate its user-controlled
	// prerequisites here, but defer that work until it has a serving Scope.
	switch strings.ToLower(opts.PreferIP) {
	case "prefer-ipv4", "prefer-ipv6", "only-ipv4", "only-ipv6":
	default:
		return nil, E.New("invalid mtproxy IP preference")
	}
	if opts.DoppelGangerEach <= 0 {
		return nil, E.New("invalid mtproxy doppelganger interval")
	}
	if opts.ThrottleCheckInterval <= 0 {
		return nil, E.New("invalid mtproxy throttle interval")
	}
	inbound.options = opts
	return inbound, nil
}

func (n *Inbound) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	n.access.Lock()
	defer n.access.Unlock()
	n.routing.RLock()
	retired := n.retired
	n.routing.RUnlock()
	if retired || scope.Context().Err() != nil {
		return net.ErrClosed
	}
	if n.proxy != nil {
		return nil
	}
	if len(n.options.Secrets) == 0 {
		n.logger.Info("mtproxy listener is inactive: no users")
		return nil
	}
	n.ctx = scope.Context()
	n.options.Network = NewNetworkAdapter(n.ctx, NewDialer(n.newConnection))
	proxy, err := mtglib.NewProxy(n.options)
	if err != nil {
		return E.New("cannot create a proxy: ", err)
	}
	n.proxy = proxy
	scope.Add(func() error {
		// Drain any callback already entering the router and reject late MTG
		// background callbacks before its asynchronous Shutdown returns.
		n.routing.Lock()
		n.retired = true
		n.routing.Unlock()
		proxy.Shutdown()
		return nil
	})
	listener, err := n.listener.ListenTCP()
	if err != nil {
		return err
	}
	served := make(chan struct{})
	scope.Add(func() error {
		err := n.listener.Close()
		<-served
		return err
	})
	go func() {
		defer close(served)
		_ = proxy.Serve(listener)
	}()
	return nil
}

func (h *Inbound) UpdateUsers(users []option.MTProxyUser) {
	secrets := make(map[string]mtglib.Secret, len(users))
	for _, user := range users {
		secret := mtglib.Secret{}
		if err := secret.Set(user.Secret); err != nil || !secret.Valid() {
			return
		}
		secrets[user.Name] = secret
	}
	h.access.Lock()
	defer h.access.Unlock()
	h.options.Secrets = secrets
	if h.proxy != nil {
		h.proxy.UpdateUsers(secrets)
	}
}

func (h *Inbound) newConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	h.routing.RLock()
	defer h.routing.RUnlock()
	if h.retired || h.ctx.Err() != nil || ctx.Err() != nil {
		_ = conn.Close()
		if onClose != nil {
			onClose(net.ErrClosed)
		}
		return
	}
	metadata.Inbound = h.Tag()
	metadata.InboundType = h.Type()
	h.logger.InfoContext(ctx, "[", metadata.User, "] inbound connection to ", metadata.Destination)
	h.router.RouteConnectionEx(ctx, conn, metadata, onClose)
}
