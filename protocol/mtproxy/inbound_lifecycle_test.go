package mtproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dolonet/mtg-multi/essentials"
	"github.com/dolonet/mtg-multi/mtglib"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
	N "github.com/sagernet/sing/common/network"
)

type mtproxyDenyRouter struct {
	adapter.Router
	calls atomic.Int32
}

func (r *mtproxyDenyRouter) RouteConnectionEx(_ context.Context, conn net.Conn, _ adapter.InboundContext, onClose N.CloseHandlerFunc) {
	r.calls.Add(1)
	_ = conn.Close()
	if onClose != nil {
		onClose(net.ErrClosed)
	}
}

func mtproxyOptions(t *testing.T, port int, users bool) option.MTProxyInboundOptions {
	t.Helper()
	var options option.MTProxyInboundOptions
	if err := json.Unmarshal([]byte(fmt.Sprintf(`{"listen":"127.0.0.1","listen_port":%d,"concurrency":1,"auto_update":true,"doppelganger_per_raid":1,"doppelganger_urls":["https://fixture.invalid/"]}`, port)), &options); err != nil {
		t.Fatal(err)
	}
	if users {
		options.Users = []option.MTProxyUser{{Name: "fixture", Secret: mtglib.GenerateSecret("fixture.invalid").Hex()}}
	}
	return options
}

func mtproxyScope(t *testing.T) *adapter.Scope {
	t.Helper()
	scope := adapter.NewScope(t.Context(), log.NewNOPFactory().Logger())
	t.Cleanup(func() { _ = scope.Close() })
	return scope
}

func closeMTProxyScope(t *testing.T, scope *adapter.Scope) {
	t.Helper()
	finished := make(chan error, 1)
	go func() { finished <- scope.Close() }()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal("MTProxy scope cleanup failed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("MTProxy scope did not drain its serving and background work")
	}
}

func TestMTProxyConstructionWaitsForServingScope(t *testing.T) {
	router := &mtproxyDenyRouter{}
	candidate, err := NewInbound(t.Context(), router, log.NewNOPFactory().Logger(), "fixture", mtproxyOptions(t, 0, true))
	if err != nil {
		t.Fatal(err)
	}
	server := candidate.(*Inbound)
	if server.proxy != nil {
		t.Fatal("unstarted constructor allocated a running MTG proxy")
	}
	time.Sleep(30 * time.Millisecond)
	if router.calls.Load() != 0 {
		t.Fatal("MTProxy construction started background network work")
	}
	scope := mtproxyScope(t)
	if err := server.Start(adapter.StartStateStart, scope); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for router.calls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("serving MTProxy never started its configured background work")
		}
		time.Sleep(time.Millisecond)
	}
	closeMTProxyScope(t, scope)
	closedCalls := router.calls.Load()
	time.Sleep(20 * time.Millisecond)
	if router.calls.Load() != closedCalls {
		t.Fatal("closed MTProxy retained background network activity")
	}
}

func TestMTProxyEmptyUsersInactiveAndInvalidOptionsStillReject(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	port := occupied.Addr().(*net.TCPAddr).Port
	router := &mtproxyDenyRouter{}
	candidate, err := NewInbound(t.Context(), router, log.NewNOPFactory().Logger(), "idle", mtproxyOptions(t, port, false))
	if err != nil {
		t.Fatal("empty MTProxy users were rejected", err)
	}
	scope := mtproxyScope(t)
	if err := candidate.Start(adapter.StartStateStart, scope); err != nil {
		t.Fatal("inactive MTProxy tried to bind an occupied port", err)
	}
	_ = occupied.Close()
	probe, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal("inactive MTProxy retained a listener", err)
	}
	_ = probe.Close()
	closeMTProxyScope(t, scope)
	closeMTProxyScope(t, scope)
	if router.calls.Load() != 0 {
		t.Fatal("inactive MTProxy started background work")
	}
	for _, field := range []string{"prefer_ip", "doppelganger_each", "throttle_check_interval"} {
		options := mtproxyOptions(t, port, false)
		switch field {
		case "prefer_ip":
			options.PreferIP = "invalid"
		case "doppelganger_each":
			options.DoppelGangerEach = badoption.Duration(-time.Second)
		case "throttle_check_interval":
			options.ThrottleCheckInterval = badoption.Duration(-time.Second)
		}
		if _, err := NewInbound(t.Context(), router, log.NewNOPFactory().Logger(), "invalid", options); err == nil {
			t.Fatal("empty users bypassed invalid MTProxy option", field)
		}
	}
	options := mtproxyOptions(t, port, true)
	options.Users[0].Secret = options.Users[0].Secret[:34]
	if _, err := NewInbound(t.Context(), router, log.NewNOPFactory().Logger(), "invalid-secret", options); err == nil || strings.Contains(err.Error(), options.Users[0].Secret) {
		t.Fatal("invalid MTProxy secret was admitted or exposed")
	}
}

func TestMTProxyServingScopeRetiresPendingPeerAndReleasesPort(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprintf("bind-conflict=%t", conflict), func(t *testing.T) {
			reserved, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer reserved.Close()
			address := reserved.Addr().String()
			port := reserved.Addr().(*net.TCPAddr).Port
			if !conflict {
				_ = reserved.Close()
			}
			candidate, err := NewInbound(t.Context(), &mtproxyDenyRouter{}, log.NewNOPFactory().Logger(), "fixture", mtproxyOptions(t, port, true))
			if err != nil {
				t.Fatal(err)
			}
			scope := mtproxyScope(t)
			err = candidate.Start(adapter.StartStateStart, scope)
			if (err != nil) != conflict {
				t.Fatal("MTProxy startup did not report its actual listener outcome", err)
			}
			var peer net.Conn
			if !conflict {
				peer, err = net.DialTimeout("tcp", address, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				defer peer.Close()
				if _, err := peer.Write([]byte{22}); err != nil {
					t.Fatal(err)
				}
				time.Sleep(5 * time.Millisecond)
			}
			closeMTProxyScope(t, scope)
			if peer != nil {
				_ = peer.SetReadDeadline(time.Now().Add(time.Second))
				if _, err := peer.Read(make([]byte, 1)); err == nil || os.IsTimeout(err) {
					t.Fatal("closed MTProxy retained a pending peer")
				}
			}
			_ = reserved.Close()
			probe, err := net.Listen("tcp", address)
			if err != nil {
				t.Fatal("MTProxy scope retained a serving socket", err)
			}
			_ = probe.Close()
			closeMTProxyScope(t, scope)
		})
	}
}

func TestMTProxyDoppelGangerIntervalIndependentOfHandshake(t *testing.T) {
	for _, handshake := range []time.Duration{0, time.Second} {
		for _, interval := range []time.Duration{0, time.Minute} {
			options := option.MTProxyInboundOptions{HandshakeTimeout: badoption.Duration(handshake), DoppelGangerEach: badoption.Duration(interval)}
			expected := interval
			if expected == 0 {
				expected = 6 * time.Hour
			}
			if options.GetDoppelGangerEach() != expected {
				t.Fatal("MTProxy ignored its configured doppelganger interval")
			}
		}
	}
}

func TestMTProxyHTTPHelperCompletesAndHonorsDialOverride(t *testing.T) {
	for _, override := range []bool{false, true} {
		t.Run(fmt.Sprintf("override=%t", override), func(t *testing.T) {
			closed := make(chan struct{}, 1)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				time.Sleep(10 * time.Millisecond)
				_, _ = io.WriteString(w, "bounded-local-http-response")
			}))
			server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateClosed {
					select {
					case closed <- struct{}{}:
					default:
					}
				}
			}
			server.Start()
			defer server.Close()
			var overrideCalls atomic.Int32
			network := NewNetworkAdapter(t.Context(), &net.Dialer{Timeout: time.Second})
			var dial func(context.Context, string, string) (essentials.Conn, error)
			if override {
				dial = func(ctx context.Context, network, address string) (essentials.Conn, error) {
					overrideCalls.Add(1)
					conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
					if err != nil {
						return nil, err
					}
					return essentials.WrapNetConn(conn), nil
				}
			}
			client := network.MakeHTTPClient(dial)
			defer client.CloseIdleConnections()
			response, err := client.Get(server.URL)
			if err != nil {
				t.Fatal("MTProxy helper rejected a valid local HTTP response", err)
			}
			body, err := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if err != nil || string(body) != "bounded-local-http-response" || override && overrideCalls.Load() != 1 {
				t.Fatal("MTProxy helper lost the response or its caller's dial instrumentation")
			}
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("MTProxy helper retained an idle control-plane connection")
			}
		})
	}
}
