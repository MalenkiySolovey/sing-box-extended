package sudoku

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/transport/sudoku/obfs/httpmask"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type ClientOptions struct {
	Context   context.Context
	Dialer    N.Dialer
	TLSConfig tls.Config
	Config    ProtocolConfig
}

type Client struct {
	dialer    N.Dialer
	tlsConfig tls.Config
	baseConf  ProtocolConfig

	muxDialer atomic.Pointer[MultiplexDialer]

	httpMaskMu     sync.Mutex
	httpMaskClient *httpmask.TunnelClient
	httpMaskKey    string

	ctx       context.Context
	cancel    context.CancelFunc
	lifeMu    sync.Mutex
	closed    bool
	closeOnce sync.Once
	pending   sync.WaitGroup
	conns     map[*clientOwnedConn]struct{}
}

func NewClient(options ClientOptions) (*Client, error) {
	parent := options.Context
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	client := &Client{
		dialer:    options.Dialer,
		tlsConfig: options.TLSConfig,
		baseConf:  options.Config,
		ctx:       ctx, cancel: cancel, conns: make(map[*clientOwnedConn]struct{}),
	}
	if client.baseConf.SessionMuxEnabled() {
		muxDialer, err := NewMultiplexDialer(client.dialMultiplexBase)
		if err != nil {
			cancel()
			return nil, err
		}
		client.muxDialer.Store(muxDialer)
	}
	return client, nil
}

func (c *Client) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if err := c.admit(); err != nil {
		return nil, err
	}
	defer c.pending.Done()
	cfg := c.baseConf
	cfg.TargetAddress = destination.String()

	if cfg.SessionMuxEnabled() && N.NetworkName(network) != N.NetworkUDP {
		muxDialer := c.muxDialer.Load()
		if muxDialer == nil {
			return nil, E.New("multiplex dialer is unavailable")
		}
		conn, err := muxDialer.Dial(ctx, cfg.TargetAddress)
		if err != nil {
			return nil, err
		}
		return c.own(conn, false)
	}

	conn, err := c.dialAndHandshake(ctx, &cfg)
	if err != nil {
		return nil, err
	}

	switch N.NetworkName(network) {
	case N.NetworkUDP:
		if err = WriteKIPMessage(conn, KIPTypeStartUoT, nil); err != nil {
			conn.Close()
			return nil, E.Cause(err, "start uot")
		}
		return c.own(conn, false)
	default:
		addrBuf, err := EncodeAddress(cfg.TargetAddress)
		if err != nil {
			conn.Close()
			return nil, E.Cause(err, "encode target address")
		}
		if err = WriteKIPMessage(conn, KIPTypeOpenTCP, addrBuf); err != nil {
			conn.Close()
			return nil, E.Cause(err, "send target address")
		}
		return c.own(conn, false)
	}
}

func (c *Client) Close() {
	c.closeOnce.Do(func() {
		c.lifeMu.Lock()
		c.closed = true
		c.cancel()
		conns := make([]*clientOwnedConn, 0, len(c.conns))
		var streams []*clientOwnedConn
		for conn := range c.conns {
			if conn.transport {
				conns = append(conns, conn)
			} else {
				streams = append(streams, conn)
			}
		}
		c.lifeMu.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
		for _, conn := range streams {
			_ = conn.Close()
		}
		if muxDialer := c.muxDialer.Swap(nil); muxDialer != nil {
			muxDialer.Close()
		}
		c.resetHTTPMaskClient()
	})
	c.pending.Wait()
}

func (c *Client) ResetConnections() error {
	if err := c.admit(); err != nil {
		return err
	}
	defer c.pending.Done()
	if c.baseConf.SessionMuxEnabled() {
		muxDialer, err := NewMultiplexDialer(c.dialMultiplexBase)
		if err != nil {
			return err
		}
		c.lifeMu.Lock()
		if c.closed || c.ctx.Err() != nil {
			c.lifeMu.Unlock()
			muxDialer.Close()
			return net.ErrClosed
		}
		previous := c.muxDialer.Swap(muxDialer)
		c.lifeMu.Unlock()
		if previous != nil {
			previous.Close()
		}
	}
	c.resetHTTPMaskClient()
	return nil
}

func (c *Client) dialAndHandshake(ctx context.Context, cfg *ProtocolConfig) (net.Conn, error) {
	handshakeCfg := *cfg
	if !handshakeCfg.DisableHTTPMask && httpTunnelModeEnabled(handshakeCfg.HTTPMaskMode) {
		handshakeCfg.DisableHTTPMask = true
	}

	upgrade := func(raw net.Conn) (net.Conn, error) {
		return ClientHandshake(raw, &handshakeCfg)
	}

	var conn net.Conn
	var err error
	var handshakeDone bool

	if !cfg.DisableHTTPMask && httpTunnelModeEnabled(cfg.HTTPMaskMode) {
		muxMode := cfg.MultiplexMode()
		if muxMode == "auto" && strings.ToLower(strings.TrimSpace(cfg.HTTPMaskMode)) != "ws" {
			if client, cerr := c.getOrCreateHTTPMaskClient(cfg); cerr == nil && client != nil {
				conn, err = client.DialTunnel(ctx, httpmask.TunnelDialOptions{
					Mode:         cfg.HTTPMaskMode,
					TLSConfig:    c.httpMaskTLSConfig(),
					HostOverride: cfg.HTTPMaskHost,
					PathRoot:     cfg.HTTPMaskPathRoot,
					AuthKey:      ClientAEADSeed(cfg.Key),
					Upgrade:      upgrade,
					DialContext:  c.dialRaw,
				})
				if err != nil {
					c.resetHTTPMaskClient()
				}
			}
		}
		if conn == nil && err == nil {
			conn, err = c.dialHTTPMaskTunnel(ctx, cfg, upgrade)
		}
		if err == nil && conn != nil {
			handshakeDone = true
		}
	}
	if conn == nil && err == nil {
		conn, err = c.dialRaw(ctx, N.NetworkTCP, cfg.ServerAddress)
	}
	if err != nil {
		return nil, E.Cause(err, "connect to ", cfg.ServerAddress)
	}

	if !handshakeDone {
		raw := conn
		conn, err = ClientHandshake(conn, &handshakeCfg)
		if err != nil {
			common.Close(raw)
			return nil, err
		}
	}

	return conn, nil
}

func (c *Client) dialMultiplexBase(ctx context.Context) (net.Conn, error) {
	if err := c.admit(); err != nil {
		return nil, err
	}
	defer c.pending.Done()
	cfg := c.baseConf
	return c.dialAndHandshake(ctx, &cfg)
}

func (c *Client) dialRaw(ctx context.Context, network, addr string) (net.Conn, error) {
	if err := c.admit(); err != nil {
		return nil, err
	}
	defer c.pending.Done()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	defer cancel()
	c.lifeMu.Lock()
	closed := c.closed || c.ctx.Err() != nil
	c.lifeMu.Unlock()
	if closed {
		return nil, net.ErrClosed
	}
	conn, err := c.dialer.DialContext(ctx, network, M.ParseSocksaddr(addr))
	if err != nil {
		return nil, err
	}
	return c.own(conn, true)
}

func (c *Client) dialHTTPMaskTunnel(ctx context.Context, cfg *ProtocolConfig, upgrade func(net.Conn) (net.Conn, error)) (net.Conn, error) {
	var earlyHandshake *httpmask.ClientEarlyHandshake
	var err error
	if upgrade != nil {
		earlyHandshake, err = newClientHTTPMaskEarlyHandshake(cfg)
		if err != nil {
			return nil, err
		}
	}
	return httpmask.DialTunnel(ctx, cfg.ServerAddress, httpmask.TunnelDialOptions{
		Mode:           cfg.HTTPMaskMode,
		TLSConfig:      c.httpMaskTLSConfig(),
		HostOverride:   cfg.HTTPMaskHost,
		PathRoot:       cfg.HTTPMaskPathRoot,
		AuthKey:        ClientAEADSeed(cfg.Key),
		EarlyHandshake: earlyHandshake,
		Upgrade:        upgrade,
		DialContext:    c.dialRaw,
	})
}

func (c *Client) httpMaskTLSConfig() tls.Config {
	if c.tlsConfig == nil {
		return nil
	}
	return c.tlsConfig
}

func (c *Client) getOrCreateHTTPMaskClient(cfg *ProtocolConfig) (*httpmask.TunnelClient, error) {
	key := cfg.ServerAddress + "|" + fmt.Sprint(c.tlsConfig != nil) + "|" + strings.TrimSpace(cfg.HTTPMaskHost)

	c.httpMaskMu.Lock()
	if c.httpMaskClient != nil && c.httpMaskKey == key {
		client := c.httpMaskClient
		c.httpMaskMu.Unlock()
		return client, nil
	}
	c.httpMaskMu.Unlock()

	client, err := httpmask.NewTunnelClient(cfg.ServerAddress, httpmask.TunnelClientOptions{
		TLSConfig:    c.httpMaskTLSConfig(),
		HostOverride: cfg.HTTPMaskHost,
		DialContext:  c.dialRaw,
		MaxIdleConns: 32,
	})
	if err != nil {
		return nil, err
	}

	c.lifeMu.Lock()
	if c.closed || c.ctx.Err() != nil {
		c.lifeMu.Unlock()
		client.CloseIdleConnections()
		return nil, net.ErrClosed
	}
	c.httpMaskMu.Lock()
	if c.httpMaskClient != nil && c.httpMaskKey == key {
		selected := c.httpMaskClient
		c.httpMaskMu.Unlock()
		c.lifeMu.Unlock()
		client.CloseIdleConnections()
		return selected, nil
	}
	previous := c.httpMaskClient
	c.httpMaskClient = client
	c.httpMaskKey = key
	c.httpMaskMu.Unlock()
	c.lifeMu.Unlock()
	if previous != nil {
		previous.CloseIdleConnections()
	}
	return client, nil
}

func (c *Client) resetHTTPMaskClient() {
	c.httpMaskMu.Lock()
	previous := c.httpMaskClient
	c.httpMaskClient = nil
	c.httpMaskKey = ""
	c.httpMaskMu.Unlock()
	if previous != nil {
		previous.CloseIdleConnections()
	}
}

func httpTunnelModeEnabled(mode string) bool {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "stream", "poll", "auto", "ws":
		return true
	default:
		return false
	}
}
