package bond

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
	"github.com/shtorm-7/go-cache/v2"
)

func RegisterInbound(registry *inbound.Registry) {
	inbound.Register[option.BondInboundOptions](registry, C.TypeBond, NewInbound)
}

type Inbound struct {
	inbound.Adapter
	logger   logger.ContextLogger
	router   adapter.ConnectionRouterEx
	inbounds []adapter.Inbound
	conns    *cache.Cache[uuid.UUID, *pendingBond]

	mtx *kmutex.Kmutex[uuid.UUID]

	connAccess  sync.Mutex
	connections map[*inboundConn]struct{}
	connDone    sync.WaitGroup
	closed      bool
	serving     context.Context
}

func NewInbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.BondInboundOptions) (adapter.Inbound, error) {
	if len(options.Inbounds) == 0 {
		return nil, E.New("missing tags")
	}
	inbound := &Inbound{
		Adapter:     inbound.NewAdapter(C.TypeBond, tag),
		logger:      logger,
		router:      uot.NewRouter(router, logger),
		mtx:         kmutex.New[uuid.UUID](),
		connections: make(map[*inboundConn]struct{}),
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
		if err := scope.Context().Err(); err != nil {
			return err
		}
		h.connAccess.Lock()
		if h.closed {
			h.connAccess.Unlock()
			return net.ErrClosed
		}
		if h.conns == nil {
			h.serving = scope.Context()
			h.conns = cache.New[uuid.UUID, *pendingBond](C.TCPConnectTimeout, time.Second)
			h.conns.OnEvicted(func(id uuid.UUID, pending *pendingBond) {
				h.mtx.Lock(id)
				legs := pending.legs
				pending.legs = nil
				h.mtx.Unlock(id)
				// Closing a leg may re-enter the runtime owner through onClose.
				for _, leg := range legs {
					_ = leg.conn.Close()
				}
			})
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

// Inbounds exposes the actual Scope-owned members for runtime inspection and
// protocol-specific controls. Callers cannot replace the serving member list.
func (h *Inbound) Inbounds() []adapter.Inbound { return slices.Clone(h.inbounds) }

func (h *Inbound) connHandler(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) (retErr error) {
	owned := &inboundConn{Conn: conn, inbound: h, onClose: onClose}
	h.connAccess.Lock()
	if h.closed || h.conns == nil {
		h.connAccess.Unlock()
		_ = owned.Close()
		return net.ErrClosed
	}
	h.connections[owned] = struct{}{}
	h.connDone.Add(1)
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(h.serving, cancel)
	owned.onClose = func(err error) {
		stop()
		cancel()
		if onClose != nil {
			onClose(err)
		}
	}
	h.connAccess.Unlock()
	finishAdmission := sync.OnceFunc(h.connDone.Done)
	defer func() {
		if retErr != nil {
			_ = owned.Close()
		}
	}()
	defer finishAdmission()
	_ = owned.SetReadDeadline(time.Now().Add(C.TCPConnectTimeout))
	request, err := ReadRequest(owned)
	_ = owned.SetReadDeadline(time.Time{})
	if err != nil {
		return err
	}
	conns, downloadRatios, uploadRatios, err := h.assemble(request, owned, metadata.User)
	// Routing and close callbacks can re-enter owner cleanup. Only the internal
	// header/assembly worker belongs to the cleanup wait group.
	finishAdmission()
	if err != nil {
		for _, leg := range conns {
			_ = leg.Close()
		}
		return err
	}
	if conns != nil {
		bonded := NewBondedConn(conns, downloadRatios, uploadRatios)
		metadata.Inbound = h.Tag()
		metadata.InboundType = C.TypeBond
		metadata.Destination = request.Destination
		h.router.RouteConnectionEx(ctx, bonded, metadata, N.OnceClose(func(error) { _ = bonded.Close() }))
	}
	return nil
}

func (h *Inbound) assemble(request *Request, conn net.Conn, user string) ([]net.Conn, []uint8, []uint8, error) {
	h.mtx.Lock(request.UUID)
	defer h.mtx.Unlock(request.UUID)
	h.connAccess.Lock()
	defer h.connAccess.Unlock()
	if h.closed {
		return nil, nil, nil, net.ErrClosed
	}
	pending, ok := h.conns.Get(request.UUID)
	if !ok {
		pending = &pendingBond{count: request.Count, destination: request.Destination, user: user, legs: make(map[uint8]*ratioConn, request.Count)}
		h.conns.SetDefault(request.UUID, pending)
	}
	if pending.complete || pending.count != request.Count || pending.destination != request.Destination || pending.user != user {
		return nil, nil, nil, E.New("conflicting bond leg")
	}
	if _, exists := pending.legs[request.Index]; exists {
		return nil, nil, nil, E.New("duplicate bond leg")
	}
	pending.legs[request.Index] = &ratioConn{
		conn:          conn,
		downloadRatio: request.DownloadRatio,
		uploadRatio:   request.UploadRatio,
	}
	if len(pending.legs) == int(request.Count) {
		conns := make([]net.Conn, len(pending.legs))
		downloadRatios := make([]uint8, len(pending.legs))
		uploadRatios := make([]uint8, len(pending.legs))
		var totalDownloadRatio, totalUploadRatio int
		for index, ratioConn := range pending.legs {
			conns[index] = ratioConn.conn
			downloadRatios[index] = ratioConn.downloadRatio
			uploadRatios[index] = ratioConn.uploadRatio
			totalDownloadRatio += int(ratioConn.downloadRatio)
			totalUploadRatio += int(ratioConn.uploadRatio)
		}
		// Keep a short-lived completed entry so replayed legs cannot replace a
		// routed bundle. Its transports now belong to the serving connection set.
		pending.complete = true
		pending.legs = nil
		if totalDownloadRatio != 100 || totalUploadRatio != 100 {
			return conns, nil, nil, E.New("invalid ratios")
		}
		return conns, downloadRatios, uploadRatios, nil
	}
	return nil, nil, nil, nil
}

type pendingBond struct {
	count       uint8
	destination M.Socksaddr
	user        string
	legs        map[uint8]*ratioConn
	complete    bool
}

type inboundConn struct {
	net.Conn
	inbound   *Inbound
	onClose   N.CloseHandlerFunc
	closeOnce sync.Once
	closeErr  error
}

func (c *inboundConn) Close() error {
	closed := false
	c.closeOnce.Do(func() {
		closed = true
		c.closeErr = c.Conn.Close()
		c.inbound.connAccess.Lock()
		delete(c.inbound.connections, c)
		c.inbound.connAccess.Unlock()
	})
	if closed && c.onClose != nil {
		c.onClose(c.closeErr)
	}
	return c.closeErr
}

func (h *Inbound) closeConnections() error {
	h.connAccess.Lock()
	if h.closed {
		h.connAccess.Unlock()
		return nil
	}
	h.closed = true
	conns := make([]*inboundConn, 0, len(h.connections))
	for conn := range h.connections {
		conns = append(conns, conn)
	}
	h.connAccess.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	if h.conns != nil {
		h.conns.Close()
	}
	h.connDone.Wait()
	return nil
}

type ratioConn struct {
	conn          net.Conn
	downloadRatio uint8
	uploadRatio   uint8
}
