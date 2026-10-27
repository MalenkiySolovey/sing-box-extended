package failover

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"

	C "github.com/sagernet/sing-box/constant"
)

type dial func() (net.Conn, error)

type failoverConn struct {
	net.Conn
	ctx     context.Context
	cancel  context.CancelFunc
	stop    func() bool
	dial    dial
	onClose func()

	readIndex    uint32
	readBuffer   *bytes.Buffer
	writeIndex   uint32
	writeBuffers [BufferSize][]byte

	await    chan struct{}
	awaitMtx sync.Mutex

	err error

	once       sync.Once
	mtx        sync.RWMutex
	readMtx    sync.Mutex
	writeMtx   sync.Mutex
	writeCall  sync.Mutex
	restoreMtx sync.Mutex
}

func NewFailoverConn(ctx context.Context, conn net.Conn, dial dial, onClose func()) *failoverConn {
	var writeBuffers [BufferSize][]byte
	for i := range BufferSize {
		writeBuffers[i] = make([]byte, 0, 1024)
	}
	lifetime, cancel := context.WithCancel(ctx)
	c := &failoverConn{
		Conn:         conn,
		ctx:          lifetime,
		cancel:       cancel,
		dial:         dial,
		readBuffer:   bytes.NewBuffer(make([]byte, 0, 1024)),
		writeBuffers: writeBuffers,
		onClose:      onClose,
	}
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	c.mtx.Lock()
	if c.err == nil {
		c.stop = stop
	} else {
		stop()
	}
	c.mtx.Unlock()
	return c
}

func (c *failoverConn) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	for {
		c.readMtx.Lock()
		c.mtx.RLock()
		conn := c.Conn
		n, err := c.read(conn, b)
		if err != nil {
			if err == SessionClosed {
				c.mtx.RUnlock()
				c.readMtx.Unlock()
				_ = c.Close()
				return 0, io.EOF
			}
			c.mtx.RUnlock()
			c.readMtx.Unlock()
			err = c.awaitConn(conn)
			if err != nil {
				return 0, err
			}
			continue
		}
		c.mtx.RUnlock()
		c.readMtx.Unlock()
		return n, err
	}
}

func (c *failoverConn) Write(b []byte) (int, error) {
	c.writeCall.Lock()
	defer c.writeCall.Unlock()
	total := 0
	for len(b) != 0 {
		frame := b[:min(len(b), 65535)]
		c.writeMtx.Lock()
		c.mtx.RLock()
		if c.err != nil || c.ctx.Err() != nil {
			err := c.err
			if err == nil {
				err = c.ctx.Err()
			}
			c.mtx.RUnlock()
			c.writeMtx.Unlock()
			return total, err
		}
		conn := c.Conn
		// Retain before writing: an errored write may already have reached the
		// peer. Its next frame acknowledgment decides whether to replay it.
		index := c.writeIndex % BufferSize
		c.writeBuffers[index] = append(c.writeBuffers[index][:0], frame...)
		c.writeIndex++
		_, err := c.write(conn, frame)
		c.mtx.RUnlock()
		c.writeMtx.Unlock()
		if err != nil {
			err = c.awaitConn(conn)
			if err != nil {
				return total, err
			}
		}
		total += len(frame)
		b = b[len(frame):]
	}
	return total, nil
}

func (c *failoverConn) RestoreConn(conn net.Conn) (retErr error) {
	c.restoreMtx.Lock()
	defer c.restoreMtx.Unlock()
	defer func() {
		if retErr != nil {
			_ = conn.Close()
		}
	}()
	if err := c.ctx.Err(); err != nil {
		return err
	}
	if err := conn.SetDeadline(time.Now().Add(C.TCPConnectTimeout)); err != nil {
		return err
	}
	stop := context.AfterFunc(c.ctx, func() { _ = conn.Close() })
	defer stop()
	c.mtx.RLock()
	old := c.Conn
	c.mtx.RUnlock()
	_ = old.Close()
	c.readMtx.Lock()
	defer c.readMtx.Unlock()
	c.writeMtx.Lock()
	replaying := false
	defer func() {
		if !replaying {
			c.writeMtx.Unlock()
		}
	}()
	c.mtx.RLock()
	err := c.err
	c.mtx.RUnlock()
	if err != nil || c.ctx.Err() != nil {
		if err != nil {
			return err
		}
		return c.ctx.Err()
	}
	ack := []byte{
		byte(c.readIndex >> 24),
		byte(c.readIndex >> 16),
		byte(c.readIndex >> 8),
		byte(c.readIndex),
	}
	// Both peers exchange an acknowledgment before replay. Read and write
	// concurrently so even an unbuffered transport can make progress.
	written := make(chan error, 1)
	go func() { _, err := writeFull(conn, ack); written <- err }()
	var data [4]byte
	_, err = io.ReadFull(conn, data[:])
	if err != nil {
		_ = conn.Close()
	}
	writeErr := <-written
	if err != nil {
		return err
	}
	if writeErr != nil {
		return writeErr
	}
	writeIndex := binary.BigEndian.Uint32(data[:])
	buffers := make([][]byte, 0, BufferSize)
	for writeIndex != c.writeIndex {
		if len(buffers) == BufferSize {
			return SessionBroken
		}
		buffers = append(buffers, c.writeBuffers[writeIndex%BufferSize])
		writeIndex++
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return err
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.err != nil {
		return c.err
	}
	if err := c.ctx.Err(); err != nil {
		return err
	}
	c.Conn = conn
	if c.await != nil {
		close(c.await)
		c.await = nil
	}
	if len(buffers) != 0 {
		// Let ordinary Read drain the peer's replay while our replay holds
		// the write side. Synchronous two-way replay can deadlock under
		// backpressure. Scope/Close still owns and interrupts this transport.
		replaying = true
		go func() {
			var err error
			for _, buffer := range buffers {
				if _, err = c.write(conn, buffer); err != nil {
					break
				}
			}
			c.writeMtx.Unlock()
			if err != nil {
				_ = conn.Close()
			}
		}()
	}
	return nil
}

func (c *failoverConn) Close() error {
	c.cancel()
	var conn net.Conn
	var notify func()
	var stop func() bool
	c.once.Do(func() {
		c.mtx.RLock()
		conn, notify, stop = c.Conn, c.onClose, c.stop
		c.mtx.RUnlock()
	})
	if conn == nil {
		return nil
	}
	if stop != nil {
		stop()
	}
	// Transport close itself may invoke an owner callback. Retire outside Once
	// as well as locks, and interrupt I/O before waiting for exclusive state.
	_ = conn.Close()
	c.mtx.Lock()
	if c.err == nil {
		c.err = io.EOF
	}
	c.mtx.Unlock()
	if notify != nil {
		notify()
	}
	return nil
}

func (c *failoverConn) fail(err error) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.err == nil {
		c.err = err
	}
	return c.err
}

func (c *failoverConn) read(conn net.Conn, b []byte) (int, error) {
	if c.readBuffer.Len() == 0 {
		c.readBuffer.Reset()
		var data [2]byte
		_, err := io.ReadFull(conn, data[:])
		if err != nil {
			return 0, err
		}
		n := binary.BigEndian.Uint16(data[:])
		if n == 0 {
			return 0, SessionClosed
		}
		_, err = io.CopyN(c.readBuffer, conn, int64(n))
		if err != nil {
			c.readBuffer.Reset()
			return 0, err
		}
		c.readIndex++
	}
	return c.readBuffer.Read(b)
}

func (c *failoverConn) write(conn net.Conn, b []byte) (int, error) {
	buffer := make([]byte, 2+len(b))
	binary.BigEndian.PutUint16(buffer, uint16(len(b)))
	copy(buffer[2:], b)
	n, err := writeFull(conn, buffer)
	return max(0, n-2), err
}

func writeFull(writer io.Writer, b []byte) (int, error) {
	total := 0
	for len(b) != 0 {
		n, err := writer.Write(b)
		if n < 0 || n > len(b) {
			return total, io.ErrShortWrite
		}
		total += n
		b = b[n:]
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

func (c *failoverConn) awaitConn(oldConn net.Conn) error {
	c.awaitMtx.Lock()
	defer c.awaitMtx.Unlock()
	c.mtx.RLock()
	err, changed := c.err, c.Conn != oldConn
	c.mtx.RUnlock()
	if err != nil {
		return err
	}
	if changed {
		return c.ctx.Err()
	}
	oldConn.Close()
	timer := time.NewTimer(C.TCPConnectTimeout)
	defer timer.Stop()
	if c.dial != nil {
		for {
			select {
			case <-c.ctx.Done():
				return c.ctx.Err()
			case <-timer.C:
				return c.fail(SessionExpired)
			default:
			}
			conn, err := c.dial()
			if err != nil {
				if err == SessionNotFound {
					return c.fail(err)
				}
				continue
			}
			err = c.RestoreConn(conn)
			if err != nil {
				if err == SessionBroken {
					return c.fail(err)
				}
				continue
			}
			return nil
		}
	} else {
		c.mtx.Lock()
		if c.Conn != oldConn {
			c.mtx.Unlock()
			return c.ctx.Err()
		}
		if c.await == nil {
			c.await = make(chan struct{})
		}
		ready := c.await
		c.mtx.Unlock()
		select {
		case <-ready:
		case <-timer.C:
			return c.fail(SessionExpired)
		case <-c.ctx.Done():
			return c.ctx.Err()
		}
	}
	return nil
}

func (c *failoverConn) LocalAddr() net.Addr {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return c.Conn.LocalAddr()
}
func (c *failoverConn) RemoteAddr() net.Addr {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return c.Conn.RemoteAddr()
}
func (c *failoverConn) SetDeadline(t time.Time) error {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return c.Conn.SetDeadline(t)
}
func (c *failoverConn) SetReadDeadline(t time.Time) error {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return c.Conn.SetReadDeadline(t)
}
func (c *failoverConn) SetWriteDeadline(t time.Time) error {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return c.Conn.SetWriteDeadline(t)
}
