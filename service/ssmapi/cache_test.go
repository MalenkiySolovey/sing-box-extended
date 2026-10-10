package ssmapi

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/common/listener"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
)

type cacheManagedFixture struct {
	inbound.Adapter
	reject bool
}

func (s *cacheManagedFixture) SetTracker(adapter.SSMTracker) {}
func (s *cacheManagedFixture) UpdateUsers([]string, []string) error {
	if s.reject {
		return errors.New("credential-must-not-appear")
	}
	return nil
}
func (s *cacheManagedFixture) Close() error                                   { return nil }
func (s *cacheManagedFixture) Start(adapter.StartStage, *adapter.Scope) error { return nil }

type cacheMemoryStore struct {
	mu     sync.Mutex
	data   []byte
	writes int
}

func (s *cacheMemoryStore) Read(context.Context) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.data...), nil
}
func (s *cacheMemoryStore) Write(ctx context.Context, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = append([]byte(nil), data...)
	s.writes++
	return nil
}

func cacheServiceFixture() *Service {
	traffic := NewTrafficManager()
	user := NewUserManager(&cacheManagedFixture{}, traffic)
	return &Service{traffics: map[string]*TrafficManager{"/main": traffic}, users: map[string]*UserManager{"/main": user}, logger: log.NewNOPFactory().Logger()}
}

func TestCacheCorruptInputBoundedAndCredentialRedacted(t *testing.T) {
	for _, data := range [][]byte{[]byte("null"), []byte(`{"endpoints":{"/main":null}}`), []byte(`{"endpoints":{"unknown":null}}`), []byte(`{"credential-must-not-appear"`), bytes.Repeat([]byte(" "), MaxCacheBytes+1)} {
		if err := ValidateCache(data); err == nil || strings.Contains(err.Error(), "credential-must-not-appear") {
			t.Fatal("corrupt cache accepted or input exposed")
		}
	}
	for _, data := range [][]byte{nil, []byte("{}"), []byte(`{"endpoints":{}}`)} {
		if err := ValidateCache(data); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCacheRestoresServiceCountersAndAuthentication(t *testing.T) {
	s := cacheServiceFixture()
	data := []byte(`{"endpoints":{"/main":{"global_uplink":17,"user_uplink":{"test-user":9},"users":{"test-user":"opaque-test-input"}}}}`)
	if err := s.decodeCache(data); err != nil {
		t.Fatal(err)
	}
	if s.traffics["/main"].globalUplink.Load() != 17 {
		t.Fatal("global service counter lost")
	}
	if s.traffics["/main"].userUplink["test-user"].Load() != 9 {
		t.Fatal("user service counter lost")
	}
	if _, found := s.users["/main"].Get("test-user"); !found {
		t.Fatal("service authentication lost")
	}
	encoded, err := s.encodeCache()
	if err != nil {
		t.Fatal(err)
	}
	restored := cacheServiceFixture()
	if err = restored.decodeCache(encoded); err != nil {
		t.Fatal(err)
	}
	if restored.traffics["/main"].globalUplink.Load() != 17 {
		t.Fatal("roundtrip changed counters")
	}
	s.users["/main"].server = &cacheManagedFixture{reject: true}
	if err = s.decodeCache(data); err == nil || strings.Contains(err.Error(), "opaque-test-input") {
		t.Fatal("authentication rejection was ignored or exposed")
	}
}

func TestCacheConcurrentUserCounterSnapshot(t *testing.T) {
	s := cacheServiceFixture()
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := 0; worker < 3; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for i := 0; i < 200; i++ {
				switch worker {
				case 0:
					if err := s.users["/main"].Update("test-user", "opaque-test-input"); err != nil {
						t.Error(err)
					}
				case 1:
					s.traffics["/main"].userCounter("test-user")
				case 2:
					if _, err := s.encodeCache(); err != nil {
						t.Error(err)
					}
				}
			}
		}(worker)
	}
	close(start)
	wg.Wait()
}

func TestCacheFailedStartPreservesPreimageAndCloseOnce(t *testing.T) {
	s := cacheServiceFixture()
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.cachePath = "owned"
	store := &cacheMemoryStore{data: []byte("null")}
	s.cacheStore = store
	if err := s.Start(adapter.StartStateStart, adapter.NewScope(context.Background(), s.logger)); err == nil {
		t.Fatal("corrupt cache accepted")
	}
	for i := 0; i < 3; i++ {
		_ = s.Close()
	}
	if store.writes != 0 || string(store.data) != "null" {
		t.Fatal("failed startup overwrote preimage")
	}
	if err := s.Start(adapter.StartStateStart, adapter.NewScope(context.Background(), s.logger)); err == nil {
		t.Fatal("closed service restarted")
	}
}

func TestCacheEmptyPathHasNoPersistence(t *testing.T) {
	s := cacheServiceFixture()
	store := &cacheMemoryStore{}
	s.cacheStore = store
	if err := s.loadCache(); err != nil {
		t.Fatal(err)
	}
	if err := s.saveCache(context.Background()); err != nil || store.writes != 0 {
		t.Fatal("absent optional cache persisted")
	}
}

func TestCacheStoreMissingFileKeepsDefault(t *testing.T) {
	s := cacheServiceFixture()
	s.ctx = context.Background()
	s.cachePath = "owned"
	s.cacheStore = missingCacheStore{}
	if err := s.loadCache(); err != nil {
		t.Fatal("missing optional cache rejected")
	}
}

type missingCacheStore struct{}

func (missingCacheStore) Read(context.Context) ([]byte, error) { return nil, os.ErrNotExist }
func (missingCacheStore) Write(context.Context, []byte) error  { return nil }

type blockedCacheStore struct {
	entered chan struct{}
	exited  chan struct{}
	once    sync.Once
	writes  int
}

func (s *blockedCacheStore) Read(context.Context) ([]byte, error) { return nil, os.ErrNotExist }
func (s *blockedCacheStore) Write(ctx context.Context, _ []byte) error {
	s.writes++
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	close(s.exited)
	return ctx.Err()
}

func TestCacheCloseBoundedConcurrentAndOnce(t *testing.T) {
	s := cacheServiceFixture()
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.cachePath = "owned"
	s.cacheReady = true
	store := &blockedCacheStore{entered: make(chan struct{}), exited: make(chan struct{})}
	s.cacheStore = store
	start := time.Now()
	results := make(chan error, 16)
	go func() { results <- s.Close() }()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("flush was not entered")
	}
	for i := 1; i < 16; i++ {
		go func() { results <- s.Close() }()
	}
	for i := 0; i < 16; i++ {
		select {
		case err := <-results:
			if err == nil {
				t.Fatal("blocked close reported success")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("close exceeded bound")
		}
	}
	select {
	case <-store.exited:
	case <-time.After(time.Second):
		t.Fatal("cancelled storage did not finish")
	}
	if store.writes != 1 || time.Since(start) > 3*time.Second {
		t.Fatal("flush repeated or close unbounded")
	}
}

type drainCacheStore struct {
	entered chan struct{}
	writes  int
}

func (s *drainCacheStore) Read(context.Context) ([]byte, error) { return nil, os.ErrNotExist }
func (s *drainCacheStore) Write(ctx context.Context, _ []byte) error {
	s.writes++
	if s.writes == 1 {
		close(s.entered)
		<-ctx.Done()
		return ctx.Err()
	}
	return ctx.Err()
}

func TestCacheLoopDrainsBeforeFinalFlush(t *testing.T) {
	s := cacheServiceFixture()
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.cachePath = "owned"
	s.cacheReady = true
	store := &drainCacheStore{entered: make(chan struct{})}
	s.cacheStore = store
	s.saveTicker = time.NewTicker(time.Millisecond)
	s.loopDone = make(chan struct{})
	go s.loopSaveCache()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("periodic write not entered")
	}
	if err := s.Start(adapter.StartStateStart, adapter.NewScope(context.Background(), s.logger)); err != nil {
		t.Fatal("repeated start", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal("close", err)
	}
	select {
	case <-s.loopDone:
	default:
		t.Fatal("writer not drained")
	}
	if store.writes != 2 {
		t.Fatal("final write did not follow drain exactly once")
	}
}

// Exercise the real HTTP/listener ownership used by a started SSM service.
// Closing the HTTP server already closes its accepted listener; the listener
// owner must still run its cleanup without turning that expected close into
// a failed hot removal of the service.
func TestCacheStartedHTTPListenerCloseAndPortReuse(t *testing.T) {
	s := cacheServiceFixture()
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.listener = listener.New(listener.Options{Context: s.ctx, Logger: s.logger, Listen: option.ListenOptions{}})
	tcp, err := s.listener.ListenTCP()
	if err != nil {
		t.Fatal("open loopback listener", err)
	}
	address := tcp.Addr().String()
	s.httpServer = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })}
	served := make(chan error, 1)
	go func() { served <- s.httpServer.Serve(tcp) }()
	t.Cleanup(func() { _ = s.Close() })
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	response, err := client.Get("http://" + address)
	if err != nil {
		t.Fatal("HTTP readiness barrier", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatal("HTTP readiness rejected")
	}
	if err = s.Close(); err != nil {
		t.Fatal("intentional service closure rejected", err)
	}
	select {
	case err = <-served:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Fatal("unexpected HTTP termination", err)
		}
	case <-time.After(time.Second):
		t.Fatal("HTTP service leaked")
	}
	reused, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal("closed listener still owns port", err)
	}
	defer reused.Close()
	results := make(chan error, 8)
	for range 8 {
		go func() { results <- s.Close() }()
	}
	for range 8 {
		select {
		case err = <-results:
			if err != nil {
				t.Fatal("repeated close changed result", err)
			}
		case <-time.After(time.Second):
			t.Fatal("repeated close blocked")
		}
	}
}
