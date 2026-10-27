package failover

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/uot"
	"github.com/sagernet/sing/service"
)

func RegisterOutbound(registry *outbound.Registry) {
	outbound.Register[option.FailoverOutboundOptions](registry, C.TypeFailover, NewFailover)
}

type Failover struct {
	outbound.Adapter
	ctx       context.Context
	outbound  adapter.OutboundManager
	outbounds []adapter.Outbound
	logger    logger.ContextLogger
	dial      DialStrategy
	uotClient *uot.Client
}

func NewFailover(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.FailoverOutboundOptions) (adapter.Outbound, error) {
	if len(options.Outbounds) == 0 {
		return nil, E.New("missing outbounds")
	}
	outboundRegistry := service.FromContext[adapter.OutboundRegistry](ctx)
	outbounds := make([]adapter.Outbound, len(options.Outbounds))
	for i, outboundOptions := range options.Outbounds {
		outbound, err := outboundRegistry.UnsafeCreateOutbound(ctx, router, logger, outboundOptions.Tag, outboundOptions.Type, outboundOptions.Options)
		if err != nil {
			return nil, err
		}
		outbounds[i] = outbound
	}
	dial, err := CreateStrategy(options.Strategy, outbounds, logger, options.Delay.Build())
	if err != nil {
		return nil, err
	}
	outbound := &Failover{
		Adapter:   outbound.NewAdapter(C.TypeFailover, tag, []string{N.NetworkTCP, N.NetworkUDP}, []string{}),
		ctx:       ctx,
		outbound:  service.FromContext[adapter.OutboundManager](ctx),
		outbounds: outbounds,
		logger:    logger,
		dial:      dial,
	}
	outbound.uotClient = &uot.Client{
		Dialer:  outbound,
		Version: uot.Version,
	}
	return outbound, nil
}

func (f *Failover) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	if stage == adapter.StartStateInitialize {
		f.ctx = scope.Context()
	}
	for _, outbound := range f.outbounds {
		lifecycle, isLifecycle := outbound.(adapter.Lifecycle)
		if !isLifecycle {
			continue
		}
		err := scope.Start("outbound/"+outbound.Type()+"["+outbound.Tag()+"]", lifecycle, stage)
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *Failover) References() []string {
	var references []string
	for _, outbound := range f.outbounds {
		referrer, isReferrer := outbound.(adapter.Referrer)
		if isReferrer {
			references = append(references, referrer.References()...)
		} else {
			references = append(references, outbound.Dependencies()...)
		}
	}
	return references
}

func (f *Failover) SetKeepIdleConnections(keep bool) {
	for _, outbound := range f.outbounds {
		keeper, isKeeper := outbound.(adapter.IdleConnectionKeeper)
		if isKeeper {
			keeper.SetKeepIdleConnections(keep)
		}
	}
}

func (f *Failover) CloseIdleConnections() {
	for _, outbound := range f.outbounds {
		keeper, isKeeper := outbound.(adapter.IdleConnectionKeeper)
		if isKeeper {
			keeper.CloseIdleConnections()
		}
	}
}

func (f *Failover) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if N.NetworkName(network) == N.NetworkUDP {
		return f.uotClient.DialContext(ctx, network, destination)
	}
	sessionUUID, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}
	sessionCtx, cancel := context.WithCancel(ctx)
	stopServing := context.AfterFunc(f.ctx, cancel)
	conn, err := f.dialSession(sessionCtx, network, &Request{Command: CommandTCP, UUID: sessionUUID, Destination: destination})
	if err != nil {
		stopServing()
		cancel()
		return nil, err
	}
	return NewFailoverConn(sessionCtx, conn, func(ctx context.Context) (net.Conn, error) {
		return f.dialSession(ctx, network, &Request{Command: CommandReconnect, UUID: sessionUUID, Destination: destination})
	}, func() { stopServing(); cancel() }), nil
}

func (f *Failover) dialSession(ctx context.Context, network string, request *Request) (conn net.Conn, retErr error) {
	ctx, cancel := context.WithTimeout(ctx, C.TCPConnectTimeout)
	defer cancel()
	conn, retErr = f.dial(ctx, network, Destination)
	if retErr != nil {
		if conn != nil {
			_ = conn.Close()
		}
		return nil, retErr
	}
	defer func() {
		if retErr != nil {
			_ = conn.Close()
		}
	}()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return conn, err
	}
	if err := WriteRequest(conn, request); err != nil {
		return conn, err
	}
	if request.Command == CommandReconnect {
		var data [1]byte
		_, err := io.ReadFull(conn, data[:])
		if err != nil {
			return conn, err
		}
		switch data[0] {
		case StatusOK:
		case StatusSessionNotFound:
			return conn, SessionNotFound
		default:
			return conn, E.New("unknown failover status: ", data[0])
		}
	}
	if err := ctx.Err(); err != nil {
		return conn, err
	}
	return conn, conn.SetDeadline(time.Time{})
}

func (f *Failover) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return f.uotClient.ListenPacket(ctx, destination)
}
