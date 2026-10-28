package v2raykcp

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"net"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/buf"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

var _ adapter.V2RayServerTransport = (*Server)(nil)

type Server struct {
	ctx        context.Context
	cancel     context.CancelFunc
	logger     logger.ContextLogger
	config     *Config
	tlsConfig  tls.ServerConfig
	handler    adapter.V2RayServerTransportHandler
	listener   *net.UDPConn
	sessions   sync.Map // map[ConnectionID]*Connection
	security   cipher.AEAD
	headerSize int
	mu         sync.Mutex
	closed     bool
	closeDone  chan struct{}
	workers    sync.WaitGroup
}

type ConnectionID struct {
	Remote string
	Port   uint16
	Conv   uint16
}

func NewServer(
	ctx context.Context,
	logger logger.ContextLogger,
	options option.V2RayKCPOptions,
	tlsConfig tls.ServerConfig,
	handler adapter.V2RayServerTransportHandler,
) (adapter.V2RayServerTransport, error) {
	config := NewConfig(options)
	if err := config.Validate(); err != nil {
		return nil, err
	}
	security, err := config.GetSecurity()
	if err != nil {
		return nil, E.Cause(err, "get security")
	}

	ctx, cancel := context.WithCancel(ctx)
	return &Server{
		ctx:        ctx,
		cancel:     cancel,
		logger:     logger,
		config:     config,
		tlsConfig:  tlsConfig,
		handler:    handler,
		security:   security,
		headerSize: HeaderSize(config.GetHeaderType()),
		closeDone:  make(chan struct{}),
	}, nil
}

func (s *Server) Network() []string {
	return []string{N.NetworkUDP}
}

func (s *Server) Serve(listener net.Listener) error {
	return E.New("KCP server requires ServePacket")
}

func (s *Server) ServePacket(listener net.PacketConn) error {
	udpConn, ok := listener.(*net.UDPConn)
	if !ok {
		return E.New("KCP requires UDP listener")
	}

	s.mu.Lock()
	if s.closed || s.listener != nil {
		s.mu.Unlock()
		return net.ErrClosed
	}
	s.listener = udpConn
	s.workers.Add(1)
	s.mu.Unlock()
	defer s.workers.Done()
	s.logger.Info("KCP server started")

	buffer := make([]byte, s.config.GetMTUValue())
	for {
		n, remoteAddr, err := udpConn.ReadFrom(buffer)
		if err != nil {
			if E.IsClosed(err) {
				return nil
			}
			return err
		}

		// Parse and admit a session before this receive buffer is reused. TLS
		// handshakes run separately so their next packet can enter that session.
		s.handlePacket(buffer[:n], remoteAddr)
	}
}

func (s *Server) handlePacket(data []byte, remoteAddr net.Addr) {
	reader := &kcpPacketReader{
		security:   s.security,
		headerSize: s.headerSize,
	}

	segments := reader.Read(data)
	if len(segments) == 0 {
		return
	}

	firstSeg := segments[0]
	conv := firstSeg.Conversation()
	cmd := firstSeg.Command()

	udpAddr, ok := remoteAddr.(*net.UDPAddr)
	if !ok {
		return
	}

	connID := ConnectionID{
		Remote: udpAddr.IP.String(),
		Port:   uint16(udpAddr.Port),
		Conv:   conv,
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		for _, segment := range segments {
			segment.Release()
		}
		return
	}
	value, exists := s.sessions.Load(connID)
	if !exists {
		if cmd == CommandTerminate {
			s.mu.Unlock()
			for _, segment := range segments {
				segment.Release()
			}
			return
		}

		// Create new connection
		writer := &serverPacketWriter{
			conn:       s.listener,
			remoteAddr: udpAddr,
			server:     s,
			connID:     connID,
			header:     s.config.GetPacketHeader(),
			security:   s.security,
		}

		meta := ConnMetadata{
			LocalAddr:    s.listener.LocalAddr(),
			RemoteAddr:   udpAddr,
			Conversation: conv,
		}

		kcpConn := NewConnection(meta, writer, writer, s.config)
		s.sessions.Store(connID, kcpConn)
		s.workers.Add(1)
		s.mu.Unlock()
		kcpConn.Input(segments)
		source := M.SocksaddrFromNet(remoteAddr)
		go func() {
			defer s.workers.Done()
			var conn net.Conn = kcpConn
			if s.tlsConfig != nil {
				tlsConn, err := tls.ServerHandshake(s.ctx, conn, s.tlsConfig)
				if err != nil {
					kcpConn.Terminate()
					return
				}
				conn = tlsConn
			}
			s.handler.NewConnectionEx(s.ctx, conn, source, M.Socksaddr{}, nil)
		}()
	} else {
		s.mu.Unlock()
		conn := value.(*Connection)
		conn.Input(segments)
	}
}

func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		<-s.closeDone
		return nil
	}
	s.closed = true
	s.cancel()
	listener := s.listener
	s.mu.Unlock()
	if listener != nil {
		_ = listener.Close()
	}
	s.sessions.Range(func(key, value interface{}) bool {
		conn := value.(*Connection)
		conn.Terminate()
		return true
	})
	s.workers.Wait()
	close(s.closeDone)
	return nil
}

type serverPacketWriter struct {
	conn       *net.UDPConn
	remoteAddr *net.UDPAddr
	server     *Server
	connID     ConnectionID
	header     PacketHeader
	security   cipher.AEAD
}

func (w *serverPacketWriter) Overhead() int {
	overhead := 0
	if w.header != nil {
		overhead += w.header.Size()
	}
	if w.security != nil {
		overhead += w.security.NonceSize() + w.security.Overhead()
	}
	return overhead
}

func (w *serverPacketWriter) Write(b []byte) (int, error) {
	buffer := buf.NewSize(len(b) + w.Overhead())
	defer buffer.Release()

	if w.header != nil {
		headerBytes := buffer.Extend(w.header.Size())
		w.header.Serialize(headerBytes)
	}

	if w.security != nil {
		nonceSize := w.security.NonceSize()
		nonce := buffer.Extend(nonceSize)
		common.Must1(rand.Read(nonce))

		encrypted := w.security.Seal(nil, nonce, b, nil)
		buffer.Write(encrypted)
	} else {
		buffer.Write(b)
	}

	_, err := w.conn.WriteTo(buffer.Bytes(), w.remoteAddr)
	return len(b), err
}

func (w *serverPacketWriter) Close() error {
	w.server.sessions.Delete(w.connID)
	return nil
}
