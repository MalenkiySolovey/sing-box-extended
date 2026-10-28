package v2raykcp

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"net"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

var _ adapter.V2RayClientTransport = (*Client)(nil)

type Client struct {
	ctx         context.Context
	cancel      context.CancelFunc
	dialer      N.Dialer
	serverAddr  M.Socksaddr
	config      *Config
	tlsConfig   tls.Config
	mu          sync.Mutex
	closed      bool
	closeDone   chan struct{}
	connections map[*Connection]struct{}
	pending     sync.WaitGroup
	readers     sync.WaitGroup
}

func NewClient(
	ctx context.Context,
	dialer N.Dialer,
	serverAddr M.Socksaddr,
	options option.V2RayKCPOptions,
	tlsConfig tls.Config,
) (adapter.V2RayClientTransport, error) {
	config := NewConfig(options)
	if err := config.Validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	return &Client{
		ctx:         ctx,
		cancel:      cancel,
		dialer:      dialer,
		serverAddr:  serverAddr,
		config:      config,
		tlsConfig:   tlsConfig,
		closeDone:   make(chan struct{}),
		connections: make(map[*Connection]struct{}),
	}, nil
}

func (c *Client) DialContext(ctx context.Context) (net.Conn, error) {
	c.mu.Lock()
	if c.closed || c.ctx.Err() != nil {
		c.mu.Unlock()
		return nil, net.ErrClosed
	}
	c.pending.Add(1)
	c.mu.Unlock()
	defer c.pending.Done()
	dialCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	defer cancel()
	// Dial UDP connection
	udpConn, err := c.dialer.DialContext(dialCtx, N.NetworkUDP, c.serverAddr)
	if err != nil {
		return nil, E.Cause(err, "dial UDP")
	}

	// Wrap as PacketConn
	packetConn := bufio.NewUnbindPacketConn(udpConn)

	// Generate conversation ID
	var convID uint16
	binary.Read(rand.Reader, binary.BigEndian, &convID)

	// Create KCP connection
	kcpConn, err := c.createConnection(packetConn, c.serverAddr.UDPAddr(), convID)
	if err != nil {
		udpConn.Close()
		return nil, E.Cause(err, "create KCP connection")
	}
	c.mu.Lock()
	if c.closed || dialCtx.Err() != nil {
		c.mu.Unlock()
		kcpConn.Terminate()
		return nil, net.ErrClosed
	}
	c.connections[kcpConn] = struct{}{}
	c.readers.Add(1)
	go c.readLoop(packetConn, kcpConn)
	c.mu.Unlock()

	// Wrap with TLS if configured
	if c.tlsConfig != nil {
		tlsConn, err := tls.ClientHandshake(dialCtx, kcpConn, c.tlsConfig)
		if err != nil {
			kcpConn.Terminate()
			return nil, E.Cause(err, "TLS handshake")
		}
		return tlsConn, nil
	}

	return kcpConn, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		<-c.closeDone
		return nil
	}
	c.closed = true
	c.cancel()
	connections := make([]*Connection, 0, len(c.connections))
	for conn := range c.connections {
		connections = append(connections, conn)
	}
	c.mu.Unlock()
	for _, conn := range connections {
		conn.Terminate()
	}
	c.pending.Wait()
	c.readers.Wait()
	close(c.closeDone)
	return nil
}

func (c *Client) createConnection(conn N.PacketConn, remoteAddr *net.UDPAddr, convID uint16) (*Connection, error) {
	security, err := c.config.GetSecurity()
	if err != nil {
		return nil, E.Cause(err, "get security")
	}

	// Create packet header
	header := c.config.GetPacketHeader()

	// Create packet writer
	writer := &kcpPacketWriter{
		conn:       conn,
		remoteAddr: remoteAddr,
		header:     header,
		security:   security,
	}

	// Create connection metadata
	meta := ConnMetadata{
		LocalAddr:    conn.LocalAddr(),
		RemoteAddr:   remoteAddr,
		Conversation: convID,
	}

	// Create KCP connection
	kcpConn := NewConnection(meta, writer, conn, c.config)

	return kcpConn, nil
}

func (c *Client) readLoop(conn N.PacketConn, kcpConn *Connection) {
	defer c.readers.Done()
	stop := context.AfterFunc(c.ctx, kcpConn.Terminate)
	defer stop()
	defer func() {
		kcpConn.Terminate()
		c.mu.Lock()
		delete(c.connections, kcpConn)
		c.mu.Unlock()
	}()
	security, _ := c.config.GetSecurity()
	reader := &kcpPacketReader{security: security, headerSize: HeaderSize(c.config.GetHeaderType())}
	for {
		buffer := buf.NewSize(int(c.config.GetMTUValue()))
		_, err := conn.ReadPacket(buffer)
		if err != nil {
			buffer.Release()
			return
		}

		segments := reader.Read(buffer.Bytes())
		buffer.Release()

		if len(segments) > 0 {
			kcpConn.Input(segments)
		}
	}
}

type kcpPacketWriter struct {
	conn       N.PacketConn
	remoteAddr *net.UDPAddr
	header     PacketHeader
	security   cipher.AEAD
}

func (w *kcpPacketWriter) Overhead() int {
	overhead := 0
	if w.header != nil {
		overhead += w.header.Size()
	}
	if w.security != nil {
		overhead += w.security.NonceSize() + w.security.Overhead()
	}
	return overhead
}

func (w *kcpPacketWriter) Write(b []byte) (int, error) {
	packet := buf.NewSize(len(b) + w.Overhead())
	defer packet.Release()

	if w.header != nil {
		headerBytes := packet.Extend(w.header.Size())
		w.header.Serialize(headerBytes)
	}

	if w.security != nil {
		nonceSize := w.security.NonceSize()
		nonce := packet.Extend(nonceSize)
		common.Must1(rand.Read(nonce))

		encrypted := w.security.Seal(nil, nonce, b, nil)
		packet.Write(encrypted)
	} else {
		packet.Write(b)
	}

	destAddr := M.SocksaddrFromNet(w.remoteAddr)
	err := w.conn.WritePacket(packet, destAddr)
	if err != nil {
		return 0, err
	}

	return len(b), nil
}

type kcpPacketReader struct {
	security   cipher.AEAD
	headerSize int
}

func (r *kcpPacketReader) Read(b []byte) []Segment {
	if r.headerSize > 0 {
		if len(b) <= r.headerSize {
			return nil
		}
		b = b[r.headerSize:]
	}

	if r.security != nil {
		nonceSize := r.security.NonceSize()
		overhead := r.security.Overhead()
		if len(b) <= nonceSize+overhead {
			return nil
		}
		out, err := r.security.Open(nil, b[:nonceSize], b[nonceSize:], nil)
		if err != nil {
			return nil
		}
		b = out
	}

	var result []Segment
	for len(b) > 0 {
		seg, extra := ReadSegment(b)
		if seg == nil {
			break
		}
		result = append(result, seg)
		b = extra
	}
	return result
}
