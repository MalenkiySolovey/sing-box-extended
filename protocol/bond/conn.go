package bond

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

type bondedConn struct {
	conns          []net.Conn
	downloadRatios []uint8
	uploadRatios   []uint8

	readAccess  sync.Mutex
	writeAccess sync.Mutex
	readBuffer  *bytes.Buffer
	readErr     error
}

func NewBondedConn(conns []net.Conn, downloadRatios, uploadRatios []uint8) *bondedConn {
	return &bondedConn{
		conns:          conns,
		downloadRatios: downloadRatios,
		uploadRatios:   uploadRatios,
		readBuffer:     bytes.NewBuffer(make([]byte, 0, 4096)),
	}
}

func (c *bondedConn) Read(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}
	c.readAccess.Lock()
	defer c.readAccess.Unlock()
	for c.readBuffer.Len() == 0 {
		if c.readErr != nil {
			return 0, c.readErr
		}
		c.readBuffer.Reset()
		var header [2]byte
		_, err := io.ReadFull(c.conns[0], header[:])
		if err != nil {
			return 0, err
		}
		size := int(binary.BigEndian.Uint16(header[:]))
		chunkLens := splitByRatios(size, c.downloadRatios)
		for i, chunkLen := range chunkLens {
			if chunkLen == 0 {
				continue
			}
			_, err := io.CopyN(c.readBuffer, c.conns[i], int64(chunkLen))
			if err != nil {
				c.readErr = err
				break
			}
		}
	}
	n, _ = c.readBuffer.Read(b)
	if c.readBuffer.Len() == 0 {
		return n, c.readErr
	}
	return n, nil
}

func (c *bondedConn) Write(b []byte) (n int, err error) {
	c.writeAccess.Lock()
	defer c.writeAccess.Unlock()
	total := 0
	for len(b) > 0 {
		// A write may exceed the uint16 frame length used on the wire.
		frame := b[:min(len(b), 65535)]
		var header [2]byte
		binary.BigEndian.PutUint16(header[:], uint16(len(frame)))
		if _, err := writeFull(c.conns[0], header[:]); err != nil {
			return total, err
		}
		offset := 0
		for i, chunkLen := range splitByRatios(len(frame), c.uploadRatios) {
			if chunkLen == 0 {
				continue
			}
			n, err := writeFull(c.conns[i], frame[offset:offset+chunkLen])
			total += n
			if err != nil {
				return total, err
			}
			offset += chunkLen
		}
		b = b[len(frame):]
	}
	return total, nil
}

func writeFull(writer io.Writer, b []byte) (int, error) {
	total := 0
	for len(b) > 0 {
		n, err := writer.Write(b)
		total += n
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
		b = b[n:]
	}
	return total, nil
}

func (c *bondedConn) Close() error {
	errs := make([]error, 0)
	for _, conn := range c.conns {
		err := conn.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (c *bondedConn) LocalAddr() net.Addr {
	return nil
}

func (c *bondedConn) RemoteAddr() net.Addr {
	return nil
}

func (c *bondedConn) SetDeadline(t time.Time) error {
	errs := make([]error, 0)
	for _, conn := range c.conns {
		err := conn.SetDeadline(t)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (c *bondedConn) SetReadDeadline(t time.Time) error {
	errs := make([]error, 0)
	for _, conn := range c.conns {
		err := conn.SetReadDeadline(t)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (c *bondedConn) SetWriteDeadline(t time.Time) error {
	errs := make([]error, 0)
	for _, conn := range c.conns {
		err := conn.SetWriteDeadline(t)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		return errors.Join(errs...)
	}
	return nil
}

func splitByRatios(number int, ratios []uint8) []int {
	result := make([]int, len(ratios))
	remaining := number
	for i := 0; i < len(ratios)-1; i++ {
		part := number * int(ratios[i]) / 100
		result[i] = part
		remaining -= part
	}
	result[len(ratios)-1] = remaining
	return result
}
