package failover

import (
	"context"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/common/kmutex"
	"github.com/sagernet/sing-box/common/uot"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

func RegisterInbound(registry *inbound.Registry) {
	inbound.Register[option.FailoverInboundOptions](registry, C.TypeFailover, NewInbound)
}

type Inbound struct {
	inbound.Adapter
	logger   logger.ContextLogger
	router   adapter.ConnectionRouterEx
	inbounds []adapter.Inbound
	conns    map[uuid.UUID]*inboundSession

	sessionMtx  *kmutex.Kmutex[uuid.UUID]
	mtx         sync.RWMutex
	connAccess  sync.Mutex
	connections map[*inboundLeg]struct{}
	connDone    sync.WaitGroup
	closed      bool
	serving     context.Context
}

type inboundSession struct {
	conn        *failoverConn
	destination M.Socksaddr
	user        string
}

func NewInbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.FailoverInboundOptions) (adapter.Inbound, error) {
	if len(options.Inbounds) == 0 {
		return nil, E.New("missing inbounds")
	}
	inbound := &Inbound{
		Adapter:     inbound.NewAdapter(C.TypeFailover, tag),
		logger:      logger,
		router:      uot.NewRouter(router, logger),
		conns:       make(map[uuid.UUID]*inboundSession),
		connections: make(map[*inboundLeg]struct{}),
		sessionMtx:  kmutex.New[uuid.UUID](),
	}
	router = NewRouter(router, logger, inbound.connHandler)
	inboundRegistry := service.FromContext[adapter.InboundRegistry](ctx)
	inbounds := make([]adapter.Inbound, len(options.Inbounds))
	for i, inboundOptions := range options.Inbounds {
		inbound, err := inboundRegistry.UnsafeCreate(ctx, router, logger, inboundOptions.Tag, inboundOptions.Type, inboundOptions.Options)
		if err != nil {
			return nil, err
		}
		inbounds[i] = inbound
	}
	inbound.inbounds = inbounds
	return inbound, nil
}

func (h *Inbound) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	if stage == adapter.StartStateInitialize {
		h.connAccess.Lock()
		if h.closed || scope.Context().Err() != nil {
			h.connAccess.Unlock()
			return net.ErrClosed
		}
		if h.serving == nil {
			h.serving = scope.Context()
			scope.Add(h.closeConnections)
		}
		h.connAccess.Unlock()
	}
	for _, inbound := range h.inbounds {
		err := scope.Start("inbound/"+inbound.Type()+"["+inbound.Tag()+"]", inbound, stage)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *Inbound) References() []string {
	var references []string
	for _, inbound := range h.inbounds {
		referrer, isReferrer := inbound.(adapter.Referrer)
		if isReferrer {
			references = append(references, referrer.References()...)
		}
	}
	return references
}

func (h *Inbound) Inbounds() []adapter.Inbound { return slices.Clone(h.inbounds) }

func (h *Inbound) connHandler(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) (retErr error) {
	if metadata.Destination != Destination {
		h.router.RouteConnectionEx(ctx, conn, metadata, onClose)
		return nil
	}
	owned := &inboundLeg{Conn: conn, inbound: h, onClose: onClose}
	h.connAccess.Lock()
	if h.closed || h.serving == nil || h.serving.Err() != nil {
		h.connAccess.Unlock()
		_ = owned.Close()
		return net.ErrClosed
	}
	h.connections[owned] = struct{}{}
	h.connDone.Add(1)
	h.connAccess.Unlock()
	finish := sync.OnceFunc(h.connDone.Done)
	defer func() {
		if retErr != nil {
			_ = owned.Close()
		}
	}()
	defer finish()
	conn = owned
	_ = conn.SetReadDeadline(time.Now().Add(C.TCPConnectTimeout))
	request, err := ReadRequest(conn)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		return err
	}
	sessionUUID := request.UUID
	h.sessionMtx.Lock(sessionUUID)
	unlockSession := sync.OnceFunc(func() { h.sessionMtx.Unlock(sessionUUID) })
	defer unlockSession()
	if request.Command == CommandTCP {
		h.mtx.RLock()
		_, exists := h.conns[sessionUUID]
		h.mtx.RUnlock()
		if exists {
			return E.New("duplicate failover session")
		}
		h.connAccess.Lock()
		if h.closed || h.serving.Err() != nil {
			h.connAccess.Unlock()
			return net.ErrClosed
		}
		// A resumable session outlives any one transport leg. Keep its accepted
		// context values, but bind cancellation to the serving Scope instead.
		routeCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		stop := context.AfterFunc(h.serving, cancel)
		session := &inboundSession{destination: request.Destination, user: metadata.User}
		h.mtx.Lock()
		session.conn = NewFailoverConn(routeCtx, conn, nil, func() {
			stop()
			cancel()
			h.mtx.Lock()
			if h.conns[sessionUUID] == session {
				delete(h.conns, sessionUUID)
			}
			h.mtx.Unlock()
		})
		h.conns[sessionUUID] = session
		h.mtx.Unlock()
		h.connAccess.Unlock()
		metadata.Inbound = h.Tag()
		metadata.InboundType = C.TypeFailover
		metadata.Destination = request.Destination
		unlockSession()
		finish()
		h.router.RouteConnectionEx(routeCtx, session.conn, metadata, N.OnceClose(func(error) { _ = session.conn.Close() }))
		return nil
	}
	if request.Command == CommandReconnect {
		h.mtx.RLock()
		session, ok := h.conns[sessionUUID]
		h.mtx.RUnlock()
		if !ok {
			unlockSession()
			_, err := conn.Write([]byte{StatusSessionNotFound})
			if err != nil {
				return err
			}
			return SessionNotFound
		}
		if session.destination != request.Destination || session.user != metadata.User {
			return E.New("conflicting failover session")
		}
		_, err = conn.Write([]byte{StatusOK})
		if err != nil {
			return err
		}
		err := session.conn.RestoreConn(conn)
		unlockSession()
		return err
	}
	return E.New("command ", request.Command, " not found")
}

type inboundLeg struct {
	net.Conn
	inbound *Inbound
	onClose N.CloseHandlerFunc
	once    sync.Once
	err     error
}

func (c *inboundLeg) Close() error {
	closed := false
	c.once.Do(func() {
		closed = true
		c.err = c.Conn.Close()
		c.inbound.connAccess.Lock()
		delete(c.inbound.connections, c)
		c.inbound.connAccess.Unlock()
	})
	if closed && c.onClose != nil {
		c.onClose(c.err)
	}
	return c.err
}

func (h *Inbound) closeConnections() error {
	h.connAccess.Lock()
	if h.closed {
		h.connAccess.Unlock()
		return nil
	}
	h.closed = true
	legs := make([]*inboundLeg, 0, len(h.connections))
	for leg := range h.connections {
		legs = append(legs, leg)
	}
	h.connAccess.Unlock()
	h.mtx.Lock()
	sessions := h.conns
	h.conns = make(map[uuid.UUID]*inboundSession)
	h.mtx.Unlock()
	for _, session := range sessions {
		_ = session.conn.Close()
	}
	for _, leg := range legs {
		_ = leg.Close()
	}
	h.connDone.Wait()
	return nil
}
