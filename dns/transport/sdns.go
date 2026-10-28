package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/ameshkov/dnscrypt/v2"
	"github.com/ameshkov/dnsstamps"
	mDNS "github.com/miekg/dns"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/dialer"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

var _ adapter.DNSTransport = (*SDNSTransport)(nil)

func RegisterSDNS(registry *dns.TransportRegistry) {
	dns.RegisterTransport[option.SDNSDNSServerOptions](registry, C.DNSTypeSDNS, NewSDNSTransport)
}

type SDNSTransport struct {
	dns.TransportAdapter
	ctx     context.Context
	cancel  context.CancelFunc
	dialer  N.Dialer
	stamp   string
	access  sync.Mutex
	closed  bool
	queries sync.WaitGroup
}

func NewSDNSTransport(ctx context.Context, logger log.ContextLogger, tag string, options option.SDNSDNSServerOptions) (adapter.DNSTransport, error) {
	stamp, err := dnsstamps.NewServerStampFromString(options.Stamp)
	if err != nil || stamp.Proto != dnsstamps.StampProtoTypeDNSCrypt || len(stamp.ServerPk) != 32 || stamp.ProviderName == "" || !M.ParseSocksaddr(stamp.ServerAddrStr).IsValid() {
		return nil, errors.New("invalid DNSCrypt resolver stamp")
	}
	transportDialer, err := dns.NewRemoteDialer(ctx, options.RemoteDNSServerOptions)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	return &SDNSTransport{
		TransportAdapter: dns.NewTransportAdapterWithRemoteOptions(C.DNSTypeSDNS, tag, options.RemoteDNSServerOptions),
		ctx:              ctx, cancel: cancel, dialer: transportDialer, stamp: options.Stamp,
	}, nil
}

func (t *SDNSTransport) Name() string {
	return t.Tag()
}

func (t *SDNSTransport) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	if stage == adapter.StartStateInitialize {
		stop := context.AfterFunc(scope.Context(), t.cancel)
		scope.Add(func() error { stop(); return t.Close() })
	}
	if stage == adapter.StartStateStart {
		return dialer.InitializeDetour(t.dialer)
	}
	return nil
}

func (t *SDNSTransport) Close() error {
	t.access.Lock()
	t.closed = true
	t.cancel()
	t.access.Unlock()
	t.queries.Wait()
	return nil
}

func (t *SDNSTransport) Reset() {
}

func (t *SDNSTransport) Exchange(ctx context.Context, message *mDNS.Msg) (*mDNS.Msg, error) {
	t.access.Lock()
	if t.closed || t.ctx.Err() != nil {
		t.access.Unlock()
		return nil, net.ErrClosed
	}
	t.queries.Add(1)
	t.access.Unlock()
	defer t.queries.Done()
	ctx, cancel := context.WithCancel(ctx)
	stopScope := context.AfterFunc(t.ctx, cancel)
	// DNSCrypt's API supplies Background to its dial hook. Bind both certificate
	// discovery and encrypted exchange to this query and the serving lifetime.
	var access sync.Mutex
	var connections []net.Conn
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(done)
		access.Lock()
		defer access.Unlock()
		for _, conn := range connections {
			_ = conn.Close()
		}
	})
	defer func() {
		stopScope()
		cancel()
		if !stop() {
			<-done
		} else {
			for _, conn := range connections {
				_ = conn.Close()
			}
		}
	}()
	client := &dnscrypt.Client{
		Net: "udp", Timeout: 10 * time.Second,
		DialContext: func(_ context.Context, network, addr string) (net.Conn, error) {
			conn, err := t.dialer.DialContext(ctx, N.NetworkName(network), M.ParseSocksaddr(addr))
			if err != nil {
				return nil, err
			}
			access.Lock()
			defer access.Unlock()
			if err := ctx.Err(); err != nil {
				_ = conn.Close()
				return nil, err
			}
			connections = append(connections, conn)
			return conn, nil
		},
	}
	resolverInfo, err := client.Dial(t.stamp)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	response, err := client.Exchange(message, resolverInfo)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return response, err
}

func (t *SDNSTransport) ExchangeAsync(ctx context.Context, message *mDNS.Msg, callback func(response *mDNS.Msg, err error)) {
	callback(t.Exchange(ctx, message))
}
