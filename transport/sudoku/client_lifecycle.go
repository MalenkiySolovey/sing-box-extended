package sudoku

import (
	"net"
	"sync"
)

func (c *Client) admit() error {
	c.lifeMu.Lock()
	defer c.lifeMu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	if err := c.ctx.Err(); err != nil {
		return err
	}
	c.pending.Add(1)
	return nil
}

// Own both selected raw transports and returned logical streams. This lets
// Close interrupt an admitted handshake as well as a fully published stream.
func (c *Client) own(conn net.Conn, transport bool) (net.Conn, error) {
	owned := &clientOwnedConn{Conn: conn, owner: c, transport: transport}
	c.lifeMu.Lock()
	if c.closed || c.ctx.Err() != nil {
		c.lifeMu.Unlock()
		_ = conn.Close()
		return nil, net.ErrClosed
	}
	c.conns[owned] = struct{}{}
	c.lifeMu.Unlock()
	return owned, nil
}

type clientOwnedConn struct {
	net.Conn
	owner     *Client
	transport bool
	once      sync.Once
	err       error
}

// Half-close belongs to the underlying protocol stream and must not retire
// the whole client-owned connection or discard its remaining read/write side.
func (c *clientOwnedConn) CloseWrite() error { return closeWrite(c.Conn) }

func (c *clientOwnedConn) CloseRead() error { return closeRead(c.Conn) }

func (c *clientOwnedConn) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		c.owner.lifeMu.Lock()
		delete(c.owner.conns, c)
		c.owner.lifeMu.Unlock()
	})
	return c.err
}
