//go:build linux && with_quic

package trusttunnel

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/netip"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
)

func TestTrustTunnelListenerClosePreservesOtherErrors(t *testing.T) {
	unexpected := errors.New("synthetic listener close failure")
	for _, fixture := range []struct {
		err  error
		fail bool
	}{{}, {err: net.ErrClosed}, {err: fmt.Errorf("tcp: %w", net.ErrClosed)},
		{err: unexpected, fail: true}, {err: errors.Join(net.ErrClosed, unexpected), fail: true}} {
		err := listenerCloseError(fixture.err)
		if fixture.fail && !errors.Is(err, unexpected) || !fixture.fail && err != nil {
			t.Fatal("listener cleanup discarded a real error or rejected an expected close", err)
		}
	}
}

func trustTunnelTestTLS(t *testing.T) *option.InboundTLSOptions {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, public, private)
	if err != nil {
		t.Fatal(err)
	}
	key, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	return &option.InboundTLSOptions{Enabled: true,
		Certificate: []string{string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))},
		Key:         []string{string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))}}
}

func TestTrustTunnelScopeOwnsServedAndPartialListeners(t *testing.T) {
	for _, fixture := range []struct {
		name, network, congestion string
		fails                     bool
	}{{name: "tcp", network: "tcp"}, {name: "udp", network: "udp"}, {name: "dual", network: "tcp\nudp"},
		{name: "partial", network: "tcp\nudp", congestion: "synthetic-unsupported", fails: true}} {
		t.Run(fixture.name, func(t *testing.T) {
			listen := badoption.Addr(netip.MustParseAddr("127.0.0.1"))
			options := option.TrustTunnelInboundOptions{ListenOptions: option.ListenOptions{Listen: &listen},
				InboundTLSOptionsContainer: option.InboundTLSOptionsContainer{TLS: trustTunnelTestTLS(t)},
				Network:                    option.NetworkList(fixture.network), CongestionController: fixture.congestion,
				Users: []option.TrustTunnelUser{{Name: "fixture", Password: "synthetic-lifecycle-only"}}}
			logger := log.NewNOPFactory().Logger()
			candidate, err := NewInbound(t.Context(), nil, logger, "fixture", options)
			if err != nil {
				t.Fatal(err)
			}
			inbound := candidate.(*Inbound)
			scope := adapter.NewScope(t.Context(), logger)
			t.Cleanup(func() {
				if err := scope.Close(); err != nil {
					t.Error(err)
				}
			})
			err = inbound.Start(adapter.StartStateStart, scope)
			if (err != nil) != fixture.fails {
				t.Fatal("unexpected TrustTunnel startup outcome", err)
			}
			addresses := map[string]string{}
			if socket := inbound.listener.TCPListener(); socket != nil {
				addresses["tcp"] = socket.Addr().String()
				if !fixture.fails {
					roots := x509.NewCertPool()
					if !roots.AppendCertsFromPEM([]byte(options.TLS.Certificate[0])) {
						t.Fatal("invalid synthetic certificate")
					}
					transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}, ForceAttemptHTTP2: true}
					defer transport.CloseIdleConnections()
					response, err := (&http.Client{Transport: transport, Timeout: 2 * time.Second}).Get("https://" + socket.Addr().String() + "/lifecycle-fixture")
					if err != nil {
						t.Fatal("real TrustTunnel TLS listener did not serve", err)
					}
					_ = response.Body.Close()
				}
			}
			if socket := inbound.listener.UDPConn(); socket != nil {
				addresses["udp"] = socket.LocalAddr().String()
			}
			if len(addresses) == 0 {
				t.Fatal("TrustTunnel fixture did not open a listener")
			}
			if err := scope.Close(); err != nil {
				t.Fatal("TrustTunnel scope rejected expected HTTP listener closure", err)
			}
			for network, address := range addresses {
				if network == "tcp" {
					socket, err := net.Listen(network, address)
					if err != nil {
						t.Fatal("TrustTunnel retained its TCP port", err)
					}
					_ = socket.Close()
				} else {
					socket, err := net.ListenPacket(network, address)
					if err != nil {
						t.Fatal("TrustTunnel retained its UDP port", err)
					}
					_ = socket.Close()
				}
			}
			if err := scope.Close(); err != nil {
				t.Fatal("TrustTunnel repeated scope close failed", err)
			}
		})
	}
}
