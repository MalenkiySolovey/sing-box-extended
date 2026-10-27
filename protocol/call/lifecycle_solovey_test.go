package call

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	transport "github.com/sagernet/sing-box/transport/call"
	"github.com/sagernet/sing-box/transport/call/tunnel"
	M "github.com/sagernet/sing/common/metadata"
)

type lifecycleResolveDialer struct{ *silentCallDialer }

func (*lifecycleResolveDialer) QueryOptions() adapter.DNSQueryOptions {
	return adapter.DNSQueryOptions{ClientSubnet: netip.MustParsePrefix("192.0.2.0/24")}
}

func TestCallServingDialerPreservesSelectedResolver(t *testing.T) {
	upstream := &lifecycleResolveDialer{&silentCallDialer{}}
	owned := newServingDialer(t.Context(), upstream)
	resolved, ok := owned.transportDialer().(dialer.ResolveDialer)
	if !ok || resolved.QueryOptions().ClientSubnet != upstream.QueryOptions().ClientSubnet {
		t.Fatal("serving transport lost the selected resolver contract")
	}
	if _, ok := newServingDialer(t.Context(), upstream.silentCallDialer).transportDialer().(dialer.ResolveDialer); ok {
		t.Fatal("serving transport invented a resolver contract")
	}
}

type lifecycleTunnel struct{}

func (*lifecycleTunnel) SendData([]byte)        {}
func (*lifecycleTunnel) SetOnData(func([]byte)) {}
func (*lifecycleTunnel) SetOnClose(func())      {}
func (*lifecycleTunnel) Reconfigure(int, int)   {}

// This proves the protocol's worker/bridge boundary, not remote platform APIs.
func TestCallInboundServingWorkerAndLateBridge(t *testing.T) {
	for _, phase := range []string{"pending", "late", "connected"} {
		t.Run(phase, func(t *testing.T) {
			logger := log.NewNOPFactory().Logger()
			scope := adapter.NewScope(t.Context(), logger)
			defer scope.Close()
			relay := tunnel.NewRelayBridge(&lifecycleTunnel{}, "creator", 32768, nil, logger)
			bridge := transport.NewBridge(relay)
			started := make(chan struct{})
			var attempts atomic.Int32
			h := &Inbound{ctx: t.Context(), logger: logger}
			h.connect = func(ctx context.Context, _ transport.Config) (*transport.Bridge, error) {
				if attempts.Add(1) != 1 {
					t.Error("duplicate serving worker")
					return nil, context.Canceled
				}
				close(started)
				if phase == "connected" {
					return bridge, nil
				}
				<-ctx.Done()
				if phase == "late" {
					return bridge, nil
				}
				return nil, ctx.Err()
			}
			for _, stage := range []adapter.StartStage{adapter.StartStateInitialize, adapter.StartStatePostStart, adapter.StartStatePostStart} {
				if err := h.Start(stage, scope); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("serving worker did not start")
			}
			if phase == "connected" {
				<-h.done
			}
			stopped := make(chan struct{})
			go func() { _ = scope.Close(); close(stopped) }()
			select {
			case <-stopped:
			case <-time.After(2 * time.Second):
				t.Fatal("serving worker outlived Scope")
			}
			if h.ctx.Err() == nil || t.Context().Err() != nil || attempts.Load() != 1 {
				t.Fatal("worker used the instance lifetime or started twice")
			}
			if phase != "pending" && !relay.IsClosed() {
				t.Fatal("connected or late bridge survived Scope")
			}
			if h.Start(adapter.StartStatePostStart, scope) == nil {
				t.Fatal("retired inbound started another worker")
			}
		})
	}
}

type silentCallDialer struct {
	conn    net.Conn
	started chan struct{}
	once    sync.Once
}

func (d *silentCallDialer) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	d.once.Do(func() { close(d.started) })
	return d.conn, nil
}

func (*silentCallDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("no packet transport in the HTTP fixture")
}

func TestCallInboundScopeInterruptsActualPlatformHTTP(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	logger := log.NewNOPFactory().Logger()
	scope := adapter.NewScope(t.Context(), logger)
	defer scope.Close()
	dialer := &silentCallDialer{conn: client, started: make(chan struct{})}
	h := &Inbound{ctx: t.Context(), logger: logger, dialer: dialer, connect: transport.Connect,
		options: option.CallInboundOptions{CallCommonOptions: option.CallCommonOptions{Platform: "telemost"}}}
	for _, stage := range []adapter.StartStage{adapter.StartStateInitialize, adapter.StartStatePostStart} {
		if err := h.Start(stage, scope); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-dialer.started:
	case <-time.After(2 * time.Second):
		t.Fatal("platform HTTP did not use its supplied dialer")
	}
	stopped := make(chan struct{})
	go func() { _ = scope.Close(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("platform HTTP blocked serving retirement")
	}
	_ = server.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := server.Read(make([]byte, 1)); err == nil {
		t.Fatal("platform authentication transport survived Scope")
	}
}
