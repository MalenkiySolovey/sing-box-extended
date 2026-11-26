package route

import (
	"net"
	"sync"

	"github.com/sagernet/sing/common/buf"
)

// A diagnostic stream may close as soon as it sees a response. Transfer
// ownership of the sniff buffers and serialize their reads and release.
func wrapGuardedStreamReplay(conn net.Conn, buffers []*buf.Buffer) net.Conn {
	if len(buffers) == 0 {
		return conn
	}
	return &guardedStreamReplay{Conn: conn, buffers: buffers}
}

type guardedStreamReplay struct {
	net.Conn
	mu      sync.Mutex
	buffers []*buf.Buffer
}

func (c *guardedStreamReplay) Read(p []byte) (int, error) {
	c.mu.Lock()
	if i := len(c.buffers) - 1; i >= 0 {
		// Native cached readers are nested, with the last buffer outermost.
		buffer := c.buffers[i]
		n := copy(p, buffer.Bytes())
		buffer.Advance(n)
		if buffer.IsEmpty() {
			c.popBuffer().Release()
		}
		c.mu.Unlock()
		return n, nil
	}
	c.mu.Unlock()
	return c.Conn.Read(p)
}

// ReadCached transfers the outermost buffer to the native write handshake.
func (c *guardedStreamReplay) ReadCached() *buf.Buffer {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.buffers) == 0 {
		return nil
	}
	return c.popBuffer()
}

func (c *guardedStreamReplay) popBuffer() *buf.Buffer {
	i := len(c.buffers) - 1
	buffer := c.buffers[i]
	c.buffers[i] = nil
	c.buffers = c.buffers[:i]
	return buffer
}

func (c *guardedStreamReplay) Close() error {
	c.mu.Lock()
	buf.ReleaseMulti(c.buffers)
	c.buffers = nil
	c.mu.Unlock()
	return c.Conn.Close()
}

func (c *guardedStreamReplay) ReaderReplaceable() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.buffers) == 0
}

func (c *guardedStreamReplay) WriterReplaceable() bool { return true }
func (c *guardedStreamReplay) Upstream() any           { return c.Conn }
