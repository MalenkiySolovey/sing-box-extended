package sudoku

import (
	"context"
	"net"
	"strings"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/common/listener"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/transport/sudoku"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func RegisterInbound(registry *inbound.Registry) {
	inbound.Register[option.SudokuInboundOptions](registry, C.TypeSudoku, NewInbound)
}

type Inbound struct {
	inbound.Adapter
	router      adapter.ConnectionRouterEx
	logger      logger.ContextLogger
	listener    *listener.Listener
	protoConf   sudoku.ProtocolConfig
	tunnelSrv   *sudoku.HTTPMaskTunnelServer
	fallback    string
	connAccess  sync.Mutex
	connections map[*inboundConn]struct{}
	connDone    sync.WaitGroup
	closed      bool
}

func NewInbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.SudokuInboundOptions) (adapter.Inbound, error) {
	defaultConf := sudoku.DefaultConfig()
	tableType, err := sudoku.NormalizeTableType(options.TableType)
	if err != nil {
		return nil, err
	}
	paddingMin, paddingMax := sudoku.ResolvePadding(options.PaddingMin, options.PaddingMax, defaultConf.PaddingMin, defaultConf.PaddingMax)
	enablePureDownlink := sudoku.DerefBool(options.EnablePureDownlink, defaultConf.EnablePureDownlink)
	handshakeTimeout := sudoku.DerefInt(options.HandshakeTimeout, defaultConf.HandshakeTimeoutSeconds)

	tables, err := sudoku.NewServerTablesWithCustomPatterns(sudoku.ServerAEADSeed(options.Key), tableType, options.CustomTable, options.CustomTables)
	if err != nil {
		return nil, err
	}

	protoConf := sudoku.ProtocolConfig{
		Key:                     options.Key,
		AEADMethod:              defaultConf.AEADMethod,
		PaddingMin:              paddingMin,
		PaddingMax:              paddingMax,
		EnablePureDownlink:      enablePureDownlink,
		HandshakeTimeoutSeconds: handshakeTimeout,
		DisableHTTPMask:         options.DisableHTTPMask,
		HTTPMaskMode:            options.HTTPMaskMode,
		HTTPMaskPathRoot:        strings.TrimSpace(options.PathRoot),
	}
	if len(tables) == 1 {
		protoConf.Table = tables[0]
	} else {
		protoConf.Tables = tables
	}
	if options.AEADMethod != "" {
		protoConf.AEADMethod = options.AEADMethod
	}
	if err := protoConf.Validate(); err != nil {
		return nil, err
	}

	inbound := &Inbound{
		Adapter:     inbound.NewAdapter(C.TypeSudoku, tag),
		router:      router,
		logger:      logger,
		protoConf:   protoConf,
		fallback:    strings.TrimSpace(options.Fallback),
		connections: make(map[*inboundConn]struct{}),
	}
	if inbound.fallback != "" {
		inbound.tunnelSrv = sudoku.NewHTTPMaskTunnelServerWithFallback(&inbound.protoConf)
	} else {
		inbound.tunnelSrv = sudoku.NewHTTPMaskTunnelServer(&inbound.protoConf)
	}

	inbound.listener = listener.New(listener.Options{
		Context:           ctx,
		Logger:            logger,
		Network:           []string{N.NetworkTCP},
		Listen:            options.ListenOptions,
		ConnectionHandler: inbound,
	})
	return inbound, nil
}

func (h *Inbound) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	scope.Add(h.closeConnections)
	err := h.listener.Start()
	if err != nil {
		return err
	}
	scope.Add(h.listener.Close)
	return nil
}

func (h *Inbound) NewConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	if onClose != nil {
		onClose = N.OnceClose(onClose)
	}
	h.connAccess.Lock()
	if h.closed {
		h.connAccess.Unlock()
		N.CloseOnHandshakeFailure(conn, onClose, net.ErrClosed)
		return
	}
	tracked := &inboundConn{Conn: conn, inbound: h, onClose: onClose}
	h.connections[tracked] = struct{}{}
	h.connDone.Add(1)
	h.connAccess.Unlock()
	defer h.connDone.Done()
	metadata.Inbound = h.Tag()
	metadata.InboundType = h.Type()
	h.handleConn(ctx, tracked, metadata, onClose)
}

// Successful TCP routing hands the connection to an asynchronous router. Keep
// ownership until the transport actually closes, not just until admission ends.
type inboundConn struct {
	net.Conn
	inbound   *Inbound
	onClose   N.CloseHandlerFunc
	closeOnce sync.Once
	closeErr  error
}

func (c *inboundConn) CloseRead() error {
	if conn, ok := c.Conn.(interface{ CloseRead() error }); ok {
		return conn.CloseRead()
	}
	return nil
}

func (c *inboundConn) CloseWrite() error {
	if conn, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return conn.CloseWrite()
	}
	return nil
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
	// Callbacks may themselves close the transport; invoke outside sync.Once.
	if closed && c.onClose != nil {
		c.onClose(c.closeErr)
	}
	return c.closeErr
}

func (h *Inbound) closeConnections() error {
	h.connAccess.Lock()
	h.closed = true
	connections := make([]*inboundConn, 0, len(h.connections))
	for conn := range h.connections {
		connections = append(connections, conn)
	}
	h.connAccess.Unlock()
	var result error
	for _, conn := range connections {
		if err := conn.Close(); err != nil && !E.IsClosedOrCanceled(err) {
			result = E.Errors(result, err)
		}
	}
	// Poll/stream sessions outlive the raw HTTP request that created them.
	result = E.Errors(result, h.tunnelSrv.Close())
	h.connDone.Wait()
	return result
}

func (h *Inbound) handleConn(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	handshakeConn := conn
	handshakeCfg := &h.protoConf

	if h.tunnelSrv != nil {
		c, cfg, done, err := h.tunnelSrv.WrapConn(conn)
		if err != nil {
			conn.Close()
			return
		}
		if done {
			return
		}
		if c != nil {
			handshakeConn = c
		}
		if cfg != nil {
			handshakeCfg = cfg
		}
	}

	cConn, meta, err := sudoku.ServerHandshake(handshakeConn, handshakeCfg)
	if err != nil {
		h.logger.DebugContext(ctx, "handshake failed: ", err)
		conn.Close()
		if handshakeConn != conn {
			handshakeConn.Close()
		}
		return
	}

	session, err := sudoku.ReadServerSession(cConn, meta)
	if err != nil {
		h.logger.WarnContext(ctx, "read session failed: ", err)
		cConn.Close()
		return
	}

	switch session.Type {
	case sudoku.SessionTypeUoT:
		h.handleUoT(ctx, session.Conn, metadata, onClose)
	case sudoku.SessionTypeMultiplex:
		mux, err := sudoku.AcceptMultiplexServer(session.Conn)
		if err != nil {
			session.Conn.Close()
			return
		}
		defer mux.Close()
		for {
			stream, target, err := mux.AcceptTCP()
			if err != nil {
				return
			}
			go h.routeTCP(ctx, stream, target, metadata)
		}
	default:
		h.routeTCP(ctx, session.Conn, session.Target, metadata)
	}
}

func (h *Inbound) routeTCP(ctx context.Context, conn net.Conn, target string, metadata adapter.InboundContext) {
	destination := M.ParseSocksaddr(target)
	metadata.Destination = destination
	h.logger.InfoContext(ctx, "inbound connection to ", destination)
	h.router.RouteConnectionEx(ctx, conn, metadata, nil)
}

func (h *Inbound) handleUoT(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	h.logger.InfoContext(ctx, "inbound packet connection")
	packetConn := sudoku.NewUoTPacketConn(conn)
	h.router.RoutePacketConnectionEx(ctx, bufio.NewPacketConn(packetConn), metadata, onClose)
}
