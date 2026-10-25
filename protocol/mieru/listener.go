package mieru

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/listener"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
)

// The Mieru API selects the port from its primary/range bindings. sing-box
// retains ownership of listen address, socket options, interface and namespace.
type mieruListenerFactory struct {
	ctx     context.Context
	logger  log.ContextLogger
	options option.ListenOptions
	scope   *adapter.Scope
	mu      sync.Mutex
	err     error
}

func (f *mieruListenerFactory) own(owned *listener.Listener) {
	// Mux.Start releases its startup barrier before the UDP underlay is
	// registered. Scope must close the actual socket even if immediate Stop
	// misses that underlay. Keep the existing listener as the resource owner.
	f.scope.Add(func() error {
		err := owned.Close()
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		return err
	})
}

func (f *mieruListenerFactory) forAddress(address string) (*listener.Listener, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("invalid Mieru listener address")
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return nil, fmt.Errorf("invalid Mieru listener port")
	}
	options := f.options
	options.ListenPort = uint16(port)
	if options.Listen == nil {
		// Preserve the Mieru API's host-dependent wildcard default. The shared
		// sing-box listener otherwise defaults an omitted address to loopback.
		addr, err := netip.ParseAddr(host)
		if err != nil {
			return nil, fmt.Errorf("invalid Mieru listener address")
		}
		listen := badoption.Addr(addr)
		options.Listen = &listen
	}
	return listener.New(listener.Options{Context: f.ctx, Logger: f.logger, Listen: options}), nil
}

func (f *mieruListenerFactory) record(err error) error {
	if err != nil {
		f.mu.Lock()
		if f.err == nil {
			f.err = err
		}
		f.mu.Unlock()
	}
	return err
}

func (f *mieruListenerFactory) failure() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.err
}

func (f *mieruListenerFactory) Listen(_ context.Context, _ string, address string) (net.Listener, error) {
	owned, err := f.forAddress(address)
	if err != nil {
		return nil, f.record(err)
	}
	socket, err := owned.ListenTCP()
	if err == nil {
		f.own(owned)
	}
	return socket, f.record(err)
}

func (f *mieruListenerFactory) ListenPacket(_ context.Context, _ string, address string) (net.PacketConn, error) {
	owned, err := f.forAddress(address)
	if err != nil {
		return nil, f.record(err)
	}
	socket, err := owned.ListenUDP()
	if err == nil {
		f.own(owned)
	}
	return socket, f.record(err)
}
