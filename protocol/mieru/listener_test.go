//go:build linux

package mieru

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	sbjson "github.com/sagernet/sing/common/json"
)

func mieruBind(network, address string) (io.Closer, error) {
	if network == "tcp" {
		return net.Listen(network, address)
	}
	return net.ListenPacket(network, address)
}

func reserveMieruPort(t *testing.T, network string) (uint16, io.Closer) {
	t.Helper()
	bound, err := mieruBind(network, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bound.Close() })
	var address net.Addr
	if listener, ok := bound.(net.Listener); ok {
		address = listener.Addr()
	} else {
		address = bound.(net.PacketConn).LocalAddr()
	}
	_, text, _ := net.SplitHostPort(address.String())
	port, _ := strconv.ParseUint(text, 10, 16)
	return uint16(port), bound
}

func startMieruListeners(t *testing.T, network, listen string, primary, extra uint16) (*Inbound, *adapter.Scope, error) {
	t.Helper()
	var options option.MieruInboundOptions
	var listenJSON string
	if listen != "" {
		listenJSON = fmt.Sprintf(`"listen":%q,`, listen)
	}
	raw := fmt.Sprintf(`{%s"listen_port":%d,"listen_ports":["%d-%d"],"transport":%q,"users":[{"name":"fixture","password":"synthetic-listener-only"}]}`, listenJSON, primary, extra, extra, strings.ToUpper(network))
	if err := sbjson.UnmarshalContext(t.Context(), []byte(raw), &options); err != nil {
		t.Fatal(err)
	}
	logger := log.NewNOPFactory().Logger()
	candidate, err := NewInbound(t.Context(), nil, logger, "fixture", options)
	if err != nil {
		t.Fatal(err)
	}
	scope := adapter.NewScope(t.Context(), logger)
	t.Cleanup(func() {
		if err := scope.Close(); err != nil {
			t.Error(err)
		}
	})
	server := candidate.(*Inbound)
	err = server.Start(adapter.StartStateStart, scope)
	return server, scope, err
}

func assertMieruPortReleased(t *testing.T, network string, port uint16) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		bound, err := mieruBind(network, net.JoinHostPort("127.0.0.1", strconv.Itoa(int(port))))
		if err == nil {
			_ = bound.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("Mieru scope retained a serving socket", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestMieruServingListenersHonorAddressAndRangePorts(t *testing.T) {
	for _, network := range []string{"tcp", "udp"} {
		t.Run(network, func(t *testing.T) {
			primary, first := reserveMieruPort(t, network)
			extra, second := reserveMieruPort(t, network)
			_ = first.Close()
			_ = second.Close()
			server, scope, err := startMieruListeners(t, network, "127.0.0.1", primary, extra)
			if err != nil || !server.server.IsRunning() {
				t.Fatal("Mieru did not reach serving state", err)
			}
			for _, port := range []uint16{primary, extra} {
				if conflict, err := mieruBind(network, net.JoinHostPort("127.0.0.1", strconv.Itoa(int(port)))); err == nil {
					_ = conflict.Close()
					t.Fatal("Mieru omitted a configured primary or range-selected port")
				}
				other, err := mieruBind(network, net.JoinHostPort("127.0.0.2", strconv.Itoa(int(port))))
				if err != nil {
					t.Fatal("loopback-only Mieru listener occupied another local address", err)
				}
				_ = other.Close()
			}
			if err := scope.Close(); err != nil || server.server.IsRunning() {
				t.Fatal("Mieru scope did not retire the server", err)
			}
			for _, port := range []uint16{primary, extra} {
				assertMieruPortReleased(t, network, port)
			}
			if err := scope.Close(); err != nil {
				t.Fatal("repeated Mieru cleanup failed", err)
			}
		})
	}
}

func TestMieruOmittedListenRetainsWildcard(t *testing.T) {
	for _, network := range []string{"tcp", "udp"} {
		t.Run(network, func(t *testing.T) {
			primary, first := reserveMieruPort(t, network)
			extra, second := reserveMieruPort(t, network)
			_ = first.Close()
			_ = second.Close()
			_, scope, err := startMieruListeners(t, network, "", primary, extra)
			if err != nil {
				t.Fatal("Mieru default listener did not start", err)
			}
			for _, port := range []uint16{primary, extra} {
				if other, err := mieruBind(network, net.JoinHostPort("127.0.0.2", strconv.Itoa(int(port)))); err == nil {
					_ = other.Close()
					t.Fatal("omitted Mieru address lost its wildcard binding")
				}
			}
			if err := scope.Close(); err != nil {
				t.Fatal(err)
			}
			assertMieruPortReleased(t, network, primary)
			assertMieruPortReleased(t, network, extra)
		})
	}
}

func TestMieruListenerFailureRejectsReadyAndCleansOtherPorts(t *testing.T) {
	for _, network := range []string{"tcp", "udp"} {
		t.Run(network, func(t *testing.T) {
			primary, first := reserveMieruPort(t, network)
			blocked, _ := reserveMieruPort(t, network)
			_ = first.Close()
			server, scope, err := startMieruListeners(t, network, "127.0.0.1", primary, blocked)
			if err == nil {
				t.Fatal("occupied Mieru port was reported ready")
			}
			if err := scope.Close(); err != nil || server.server.IsRunning() {
				t.Fatal("failed Mieru startup did not retire its partial server", err)
			}
			assertMieruPortReleased(t, network, primary)
		})
	}
}
