package hysteria2

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	boxService "github.com/sagernet/sing-box/adapter/service"
	"github.com/sagernet/sing-box/common/listener"
	"github.com/sagernet/sing-box/common/tls"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	N "github.com/sagernet/sing/common/network"
	aTLS "github.com/sagernet/sing/common/tls"
	sHTTP "github.com/sagernet/sing/protocol/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/render"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c" //nolint:staticcheck
)

func RegisterRealmService(registry *boxService.Registry) {
	boxService.Register[option.HysteriaRealmServiceOptions](registry, C.TypeHysteriaRealm, NewRealmService)
}

type RealmService struct {
	boxService.Adapter
	ctx              context.Context
	cancel           context.CancelFunc
	logger           log.ContextLogger
	listener         *listener.Listener
	tlsConfig        tls.ServerConfig
	httpServer       *http.Server
	server           *server
	connectionAccess sync.Mutex
	connections      map[*realmConn]struct{}
	serveDone        chan struct{}
	handlers         sync.WaitGroup
}

func NewRealmService(ctx context.Context, logger log.ContextLogger, tag string, options option.HysteriaRealmServiceOptions) (adapter.Service, error) {
	if len(options.Users) == 0 {
		return nil, E.New("missing users")
	}
	tokenMap := make(map[string]*realmUser, len(options.Users))
	for i, user := range options.Users {
		if user.Name == "" {
			return nil, E.New("missing name for user[", i, "]")
		}
		if user.Token == "" {
			return nil, E.New("missing token for user[", i, "]")
		}
		tokenMap[user.Token] = &realmUser{
			name:      user.Name,
			maxRealms: user.MaxRealms,
		}
	}
	server := newServer(logger, tokenMap)
	ctx, cancel := context.WithCancel(ctx)
	chiRouter := chi.NewRouter()
	chiRouter.Use(middleware.RequestSize(maxRequestBodyBytes))
	chiRouter.Use(func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger.DebugContext(r.Context(), r.Method, " ", r.RequestURI, " ", sHTTP.SourceAddress(r))
			handler.ServeHTTP(w, r)
		})
	})
	chiRouter.Route("/v1/{id}", func(r chi.Router) {
		r.Use(validateRealmID)
		r.With(server.authUser).Post("/", server.handleRegister)
		r.With(server.authSession).Delete("/", server.handleDeregister)
		r.With(server.authSession).Get("/events", server.handleEvents)
		r.With(server.authSession).Post("/heartbeat", server.handleHeartbeat)
		r.With(server.authUser).Post("/connect", server.handleConnect)
		r.With(server.authSession).Post("/connects/{nonce}", server.handleConnectResponse)
	})
	chiRouter.NotFound(func(w http.ResponseWriter, r *http.Request) {
		render.Status(r, http.StatusNotFound)
		render.JSON(w, r, render.M{"error": "not_found", "message": "unknown path"})
	})
	chiRouter.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		render.Status(r, http.StatusMethodNotAllowed)
		render.JSON(w, r, render.M{"error": "bad_request", "message": "method not allowed"})
	})
	s := &RealmService{
		Adapter: boxService.NewAdapter(C.TypeHysteriaRealm, tag),
		ctx:     ctx,
		cancel:  cancel,
		logger:  logger,
		listener: listener.New(listener.Options{
			Context: ctx,
			Logger:  logger,
			Network: []string{N.NetworkTCP},
			Listen:  options.ListenOptions,
		}),
		httpServer: &http.Server{
			BaseContext: func(net.Listener) context.Context { return ctx },
			//nolint:staticcheck
			Handler: h2c.NewHandler(chiRouter, &http2.Server{
				IdleTimeout:                  time.Duration(options.IdleTimeout),
				ReadIdleTimeout:              time.Duration(options.KeepAlivePeriod),
				MaxUploadBufferPerStream:     int32(options.StreamReceiveWindow.Value()),
				MaxUploadBufferPerConnection: int32(options.ConnectionReceiveWindow.Value()),
				MaxConcurrentStreams:         uint32(options.MaxConcurrentStreams),
			}),
			ConnContext: func(ctx context.Context, _ net.Conn) context.Context {
				return log.ContextWithNewID(ctx)
			},
		},
		server:      server,
		connections: make(map[*realmConn]struct{}),
	}
	handler := s.httpServer.Handler
	s.httpServer.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The same gate orders handler admission against shutdown's snapshot.
		// h2c ServeHTTP owns its hijacked connection until ServeConn returns.
		s.connectionAccess.Lock()
		if s.ctx.Err() != nil {
			s.connectionAccess.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		s.handlers.Add(1)
		s.connectionAccess.Unlock()
		defer s.handlers.Done()
		handler.ServeHTTP(w, r)
	})
	if options.TLS != nil {
		tlsConfig, err := tls.NewServer(ctx, logger, common.PtrValueOrDefault(options.TLS))
		if err != nil {
			return nil, err
		}
		s.tlsConfig = tlsConfig
	}
	return s, nil
}

func (s *RealmService) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	if stage == adapter.StartStateInitialize {
		scope.Add(func() error {
			s.cancel()
			return nil
		})
		return nil
	}
	if stage != adapter.StartStateStart {
		return nil
	}
	if s.tlsConfig != nil {
		err := s.tlsConfig.Start()
		if err != nil {
			return E.Cause(err, "create TLS config")
		}
		scope.Add(s.tlsConfig.Close)
	}
	tcpListener, err := s.listener.ListenTCP()
	if err != nil {
		return err
	}
	scope.Add(func() error {
		err := s.listener.Close()
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		return err
	})
	// h2c hijacks its TCP connection, so http.Server.Close alone cannot
	// retire it. Track the accepted socket before either HTTP or TLS owns it.
	tcpListener = &realmListener{Listener: tcpListener, owner: s}
	if s.tlsConfig != nil {
		if !common.Contains(s.tlsConfig.NextProtos(), http2.NextProtoTLS) {
			s.tlsConfig.SetNextProtos(append([]string{"h2"}, s.tlsConfig.NextProtos()...))
		}
		tcpListener = aTLS.NewListener(tcpListener, s.tlsConfig)
	}
	s.serveDone = make(chan struct{})
	go func() {
		defer close(s.serveDone)
		err := s.httpServer.Serve(tcpListener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) && s.ctx.Err() == nil {
			s.logger.Error("serve error: ", err)
		}
	}()
	scope.Add(s.closeHTTP)
	return nil
}

func (s *RealmService) closeHTTP() error {
	s.cancel()
	s.server.closeAll()
	s.connectionAccess.Lock()
	connections := make([]*realmConn, 0, len(s.connections))
	for conn := range s.connections {
		connections = append(connections, conn)
	}
	s.connectionAccess.Unlock()
	var result error
	for _, conn := range connections {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			result = errors.Join(result, err)
		}
	}
	if err := s.httpServer.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		result = errors.Join(result, err)
	}
	<-s.serveDone
	s.handlers.Wait()
	return result
}

type realmListener struct {
	net.Listener
	owner *RealmService
}

func (l *realmListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	tracked := &realmConn{Conn: conn, owner: l.owner}
	l.owner.connectionAccess.Lock()
	if l.owner.ctx.Err() != nil {
		l.owner.connectionAccess.Unlock()
		_ = conn.Close()
		return nil, net.ErrClosed
	}
	l.owner.connections[tracked] = struct{}{}
	l.owner.connectionAccess.Unlock()
	return tracked, nil
}

type realmConn struct {
	net.Conn
	owner *RealmService
	once  sync.Once
	err   error
}

func (c *realmConn) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		c.owner.connectionAccess.Lock()
		delete(c.owner.connections, c)
		c.owner.connectionAccess.Unlock()
	})
	return c.err
}
