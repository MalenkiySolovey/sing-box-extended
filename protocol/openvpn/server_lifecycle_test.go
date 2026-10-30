package openvpn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	ovpntransport "github.com/sagernet/sing-box/transport/openvpn"
)

type lifecycleDevice struct {
	ovpntransport.Device
	startErr error
	closed   bool
}

func (d *lifecycleDevice) Start() error { return d.startErr }
func (d *lifecycleDevice) Close() error { d.closed = true; return nil }

func TestOpenVPNServerScopeOwnsTransportRetirement(t *testing.T) {
	for _, network := range []string{"tcp", "udp"} {
		for _, failStart := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/failed_start=%t", network, failStart), func(t *testing.T) {
				var options option.OpenVPNServerEndpointOptions
				staticKey := "-----BEGIN OpenVPN Static key V1-----\n" + strings.Repeat("01", 256) + "\n-----END OpenVPN Static key V1-----"
				raw := fmt.Sprintf(`{"listen":"127.0.0.1","network":%q,"mode":"static_key","address":["10.88.0.1/24"],"peer_address":"10.88.0.2","cipher":"AES-256-CBC","static_key":[%q]}`, network, staticKey)
				if err := json.Unmarshal([]byte(raw), &options); err != nil {
					t.Fatal(err)
				}
				if network == "udp" {
					options.Remote = "127.0.0.1"
					options.RemotePort = 1194
				}
				serverOptions, err := buildServerOptions(options)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				t.Cleanup(cancel)
				logger := log.NewNOPFactory().Logger()
				serverOptions.Context = ctx
				serverOptions.Logger = logger
				device := &lifecycleDevice{}
				if failStart {
					device.startErr = errors.New("device start rejected")
				}
				endpoint := &ServerEndpoint{endpointBase: endpointBase{logger: logger}, ctx: ctx, cancelLoop: cancel, options: options, serverOptions: serverOptions, device: device}
				scope := adapter.NewScope(ctx, logger)
				t.Cleanup(func() { _ = scope.Close() })
				if err := endpoint.Start(adapter.StartStateInitialize, scope); err != nil {
					t.Fatal(err)
				}
				err = endpoint.Start(adapter.StartStateStart, scope)
				if failStart {
					if !errors.Is(err, device.startErr) {
						t.Fatal("lost start error", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				var address string
				if network == "tcp" {
					address = endpoint.listener.TCPListener().Addr().String()
				} else {
					address = endpoint.listener.UDPConn().LocalAddr().String()
				}
				if err := scope.Close(); err != nil {
					t.Fatal("ordinary scope retirement failed", err)
				}
				if !device.closed || endpoint.started.Load() {
					t.Fatal("retired endpoint retained device authority")
				}
				if err := scope.Close(); err != nil {
					t.Fatal("repeated close failed", err)
				}
				if network == "tcp" {
					listener, err := net.Listen(network, address)
					if err != nil {
						t.Fatal("listener retained after scope close", err)
					}
					_ = listener.Close()
				} else {
					listener, err := net.ListenPacket(network, address)
					if err != nil {
						t.Fatal("packet socket retained after scope close", err)
					}
					_ = listener.Close()
				}
			})
		}
	}
}
