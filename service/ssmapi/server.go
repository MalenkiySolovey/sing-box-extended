package ssmapi

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
	"github.com/sagernet/sing/service"

	"github.com/go-chi/chi/v5"
	"golang.org/x/net/http2"
)

func RegisterService(registry *boxService.Registry) {
	boxService.Register[option.SSMAPIServiceOptions](registry, C.TypeSSMAPI, NewService)
}

type Service struct {
	boxService.Adapter
	ctx            context.Context
	cancel         context.CancelFunc
	logger         log.ContextLogger
	listener       *listener.Listener
	tlsConfig      tls.ServerConfig
	httpServer     *http.Server
	traffics       map[string]*TrafficManager
	users          map[string]*UserManager
	cachePath      string
	saveTicker     *time.Ticker
	lastSavedCache []byte
	cacheMutex     sync.Mutex
	cacheStore     CacheStore
	cacheReady     bool
	loopDone       chan struct{}
	lifecycle      sync.Mutex
	closeOnce      sync.Once
	closeErr       error
	closed         bool
}

func NewService(ctx context.Context, logger log.ContextLogger, tag string, options option.SSMAPIServiceOptions) (adapter.Service, error) {
	ctx, cancel := context.WithCancel(ctx)
	chiRouter := chi.NewRouter()
	s := &Service{
		Adapter: boxService.NewAdapter(C.TypeSSMAPI, tag),
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
			Handler: chiRouter,
		},
		traffics:  make(map[string]*TrafficManager),
		users:     make(map[string]*UserManager),
		cachePath: options.CachePath,
	}
	s.cacheStore, _ = ctx.Value(cacheStoreKey{}).(CacheStore)
	if s.cacheStore == nil {
		s.cacheStore = fileCacheStore{ctx: ctx, path: options.CachePath}
	}
	inboundManager := service.FromContext[adapter.InboundManager](ctx)
	if options.Servers.Size() == 0 {
		return nil, E.New("missing servers")
	}
	for i, entry := range options.Servers.Entries() {
		inbound, loaded := inboundManager.Get(entry.Value)
		if !loaded {
			return nil, E.New("parse SSM server[", i, "]: inbound ", entry.Value, " not found")
		}
		managedServer, isManaged := inbound.(adapter.ManagedSSMServer)
		if !isManaged {
			return nil, E.New("parse SSM server[", i, "]: inbound/", inbound.Type(), "[", inbound.Tag(), "] is not a SSM server")
		}
		traffic := NewTrafficManager()
		managedServer.SetTracker(traffic)
		user := NewUserManager(managedServer, traffic)
		chiRouter.Route(entry.Key, NewAPIServer(logger, traffic, user).Route)
		s.traffics[entry.Key] = traffic
		s.users[entry.Key] = user
	}
	if options.TLS != nil {
		tlsConfig, err := tls.NewServer(ctx, logger, common.PtrValueOrDefault(options.TLS))
		if err != nil {
			return nil, err
		}
		s.tlsConfig = tlsConfig
	}
	return s, nil
}

func (s *Service) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	if s.closed {
		return errors.New("SSM_SERVICE_CLOSED")
	}
	if s.cacheReady {
		return nil
	}
	// Scope owns cleanup, including startup failure. Close remains idempotent
	// so callers may also explicitly drain this service before removing it.
	scope.Add(s.Close)
	err := s.loadCache()
	if err != nil {
		return errors.New("SSM_CACHE_LOAD_REJECTED")
	}
	if s.tlsConfig != nil {
		err = s.tlsConfig.Start()
		if err != nil {
			return E.Cause(err, "create TLS config")
		}
	}
	tcpListener, err := s.listener.ListenTCP()
	if err != nil {
		return err
	}
	if s.tlsConfig != nil {
		if !common.Contains(s.tlsConfig.NextProtos(), http2.NextProtoTLS) {
			s.tlsConfig.SetNextProtos(append([]string{"h2"}, s.tlsConfig.NextProtos()...))
		}
		tcpListener = aTLS.NewListener(tcpListener, s.tlsConfig)
	}
	go func() {
		err = s.httpServer.Serve(tcpListener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("serve error: ", err)
		}
	}()
	s.cacheReady = true
	if s.cachePath != "" {
		s.saveTicker = time.NewTicker(1 * time.Minute)
		s.loopDone = make(chan struct{})
		go s.loopSaveCache()
	}
	return nil
}

func (s *Service) loopSaveCache() {
	defer close(s.loopDone)
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.saveTicker.C:
			err := s.saveCache(s.ctx)
			if err != nil {
				s.logger.Error(E.Cause(err, "save cache"))
			}
		}
	}
}

func (s *Service) Close() error {
	s.closeOnce.Do(func() {
		s.lifecycle.Lock()
		s.closed = true
		s.cancel()
		if s.saveTicker != nil {
			s.saveTicker.Stop()
		}
		ready, done := s.cacheReady, s.loopDone
		s.lifecycle.Unlock()
		// http.Server.Close closes the same TCP listener it serves. Retain the
		// listener owner's cleanup, but an already-closed socket is expected at
		// this seam and must not turn a normal hot removal into a failed reload.
		s.closeErr = common.Close(common.PtrOrNil(s.httpServer))
		listenerErr := common.Close(common.PtrOrNil(s.listener))
		if errors.Is(listenerErr, net.ErrClosed) {
			listenerErr = nil
		}
		s.closeErr = errors.Join(s.closeErr, listenerErr, common.Close(s.tlsConfig))
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if done != nil {
			select {
			case <-done:
			case <-ctx.Done():
				s.closeErr = errors.Join(s.closeErr, errors.New("SSM_CACHE_DRAIN_TIMEOUT"))
				return
			}
		}
		if ready && s.cachePath != "" {
			flush := make(chan error, 1)
			go func() { flush <- s.saveCache(ctx) }()
			select {
			case err := <-flush:
				s.closeErr = errors.Join(s.closeErr, err)
			case <-ctx.Done():
				s.closeErr = errors.Join(s.closeErr, errors.New("SSM_CACHE_DRAIN_TIMEOUT"))
			}
		}
	})
	return s.closeErr
}
