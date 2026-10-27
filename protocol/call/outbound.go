package call

import (
	"context"
	"net"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/dialer"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/transport/call"
	"github.com/sagernet/sing-box/transport/call/common"
	"github.com/sagernet/sing/common/bufio"
	"github.com/sagernet/sing/common/bufio/deadline"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

func RegisterOutbound(registry *outbound.Registry) {
	outbound.Register[option.CallOutboundOptions](registry, C.TypeCall, NewOutbound)
}

type Outbound struct {
	outbound.Adapter
	ctx         context.Context
	logger      logger.ContextLogger
	options     option.CallOutboundOptions
	dialer      N.Dialer
	network     *servingDialer
	bridge      *call.Bridge
	access      sync.Mutex
	initialized bool
	closed      bool
	done        chan struct{}
	connect     func(context.Context, call.Config) (*call.Bridge, error)
}

func NewOutbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.CallOutboundOptions) (adapter.Outbound, error) {
	if err := validateCommonOptions(options.CallCommonOptions, false); err != nil {
		return nil, err
	}
	if err := validateCredentials(options.Cookies, options.JoinLink, false); err != nil {
		return nil, err
	}
	outboundDialer, err := dialer.New(ctx, options.DialerOptions, true)
	if err != nil {
		return nil, err
	}
	return &Outbound{
		Adapter: outbound.NewAdapterWithDialerOptions(C.TypeCall, tag, []string{N.NetworkTCP, N.NetworkUDP}, options.DialerOptions),
		ctx:     ctx,
		logger:  logger,
		options: options,
		dialer:  outboundDialer,
		connect: call.Connect,
	}, nil
}

func (o *Outbound) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	o.access.Lock()
	defer o.access.Unlock()
	if o.closed || scope.Context().Err() != nil {
		return net.ErrClosed
	}
	if stage == adapter.StartStateInitialize && !o.initialized {
		o.ctx = scope.Context()
		o.network = newServingDialer(o.ctx, o.dialer)
		o.initialized = true
		scope.Add(o.close)
	}
	if stage == adapter.StartStatePostStart && o.done == nil {
		if !o.initialized {
			return E.New("call outbound not initialized")
		}
		o.done = make(chan struct{})
		go func() { defer close(o.done); o.run() }()
	}
	return nil
}

func (o *Outbound) close() error {
	o.access.Lock()
	o.closed = true
	bridge, done, network := o.bridge, o.done, o.network
	o.access.Unlock()
	if network != nil {
		network.Close()
	}
	var err error
	if bridge != nil {
		err = bridge.Close()
	}
	if done != nil {
		<-done
	}
	return err
}

func (o *Outbound) run() {
	bridge, err := o.connect(o.ctx, call.Config{
		Platform:     o.options.Platform,
		Mode:         o.options.Mode,
		JoinLink:     o.options.JoinLink,
		CookieString: o.options.Cookies.Header(),
		ReadBuffer:   o.options.ReadBuffer,
		Role:         call.RoleJoiner,
		Dialer:       o.network.transportDialer(),
		DNSRouter:    service.FromContext[adapter.DNSRouter](o.ctx),
		Logger:       o.logger,
	})
	if err != nil {
		o.logger.ErrorContext(o.ctx, common.MaskError(err))
		return
	}
	o.access.Lock()
	if o.closed || o.ctx.Err() != nil {
		o.access.Unlock()
		_ = bridge.Close()
		return
	}
	o.bridge = bridge
	o.access.Unlock()
}

func (o *Outbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	bridge, err := o.awaitBridge(ctx)
	if err != nil {
		return nil, err
	}
	switch N.NetworkName(network) {
	case N.NetworkTCP:
		o.logger.InfoContext(ctx, "outbound connection to ", destination)
		conn, err := bridge.DialContext(ctx, destination.String())
		if err != nil {
			return nil, err
		}
		return deadline.NewConn(conn), nil
	default:
		return nil, E.New("call: unsupported network: ", network)
	}
}

func (o *Outbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	bridge, err := o.awaitBridge(ctx)
	if err != nil {
		return nil, err
	}
	o.logger.InfoContext(ctx, "outbound packet connection to ", destination)
	conn, err := bridge.ListenPacket(ctx, destination.String())
	if err != nil {
		return nil, err
	}
	return bufio.NewUnbindPacketConnWithAddr(conn, destination), nil
}

func (o *Outbound) awaitBridge(ctx context.Context) (*call.Bridge, error) {
	o.access.Lock()
	done, serving := o.done, o.ctx
	closed := o.closed
	o.access.Unlock()
	if closed {
		return nil, net.ErrClosed
	}
	if done == nil {
		return nil, E.New("call outbound not started")
	}
	select {
	case <-done:
	case <-serving.Done():
		return nil, net.ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	o.access.Lock()
	defer o.access.Unlock()
	if o.closed || serving.Err() != nil {
		return nil, net.ErrClosed
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if o.bridge == nil {
		return nil, E.New("call: tunnel not initialized")
	}
	return o.bridge, nil
}
