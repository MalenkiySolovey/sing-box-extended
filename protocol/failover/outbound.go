package failover

import (
	"context"
	"io"
	"net"
	"sync"
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
	access    sync.Mutex
	closed    bool
	dials     sync.WaitGroup
	sessions  map[*failoverConn]chan struct{}
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
		scope.Add(f.close)
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

func (f *Failover) close() error {
	f.access.Lock()
	f.closed = true
	sessions := make(map[*failoverConn]chan struct{}, len(f.sessions))
	for conn, done := range f.sessions {
		sessions[conn] = done
	}
	f.access.Unlock()
	// Cancellation callbacks run asynchronously. Retire and join this parent's
	// streams before Scope returns; canonical borrowed outbounds stay owned by
	// their manager. A Close already in progress still owes its completion.
	for conn, done := range sessions {
		_ = conn.Close()
		<-done
	}
	f.dials.Wait()
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
	f.access.Lock()
	if f.closed || f.ctx.Err() != nil {
		f.access.Unlock()
		return nil, net.ErrClosed
	}
	f.dials.Add(1)
	f.access.Unlock()
	defer f.dials.Done()
	sessionUUID, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}
	// The caller's context bounds dialing, as with net.Dialer. Once connected,
	// the resumable stream belongs to its serving Scope and explicit Close.
	dialCtx, cancelDial := context.WithCancel(ctx)
	stopDial := context.AfterFunc(f.ctx, cancelDial)
	conn, err := f.dialSession(dialCtx, network, &Request{Command: CommandTCP, UUID: sessionUUID, Destination: destination})
	stopDial()
	cancelDial()
	if err != nil {
		return nil, err
	}
	sessionCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stopServing := context.AfterFunc(f.ctx, cancel)
	f.access.Lock()
	if f.closed || f.ctx.Err() != nil {
		f.access.Unlock()
		stopServing()
		cancel()
		_ = conn.Close()
		return nil, net.ErrClosed
	}
	done := make(chan struct{})
	var session *failoverConn
	session = NewFailoverConn(sessionCtx, conn, func(ctx context.Context) (net.Conn, error) {
		return f.dialSession(ctx, network, &Request{Command: CommandReconnect, UUID: sessionUUID, Destination: destination})
	}, func() {
		defer close(done)
		stopServing()
		cancel()
		f.access.Lock()
		delete(f.sessions, session)
		f.access.Unlock()
	})
	if f.sessions == nil {
		f.sessions = make(map[*failoverConn]chan struct{})
	}
	f.sessions[session] = done
	f.access.Unlock()
	return session, nil
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
