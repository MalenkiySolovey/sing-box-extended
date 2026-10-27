package failover

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
)

type cutFailoverConn struct {
	net.Conn
	limit int
	cut   chan struct{}
	once  sync.Once
}

type shortFailoverConn struct{ net.Conn }

func (c *shortFailoverConn) Write(b []byte) (int, error) {
	return c.Conn.Write(b[:min(len(b), 97)])
}

func (c *cutFailoverConn) Write(b []byte) (int, error) {
	n := 0
	if c.limit > 0 {
		var err error
		n, err = c.Conn.Write(b[:min(c.limit, len(b))])
		if err != nil {
			return n, err
		}
	}
	_ = c.Conn.Close()
	c.once.Do(func() { close(c.cut) })
	return n, io.ErrClosedPipe
}

func TestFailoverFramingAndBidirectionalResume(t *testing.T) {
	for _, both := range []bool{false, true} {
		t.Run(map[bool]string{false: "partial-frame", true: "two-way-replay"}[both], func(t *testing.T) {
			rawLeft, rawRight := net.Pipe()
			cutLeft := &cutFailoverConn{Conn: rawLeft, limit: 1000, cut: make(chan struct{})}
			var rightConn net.Conn = rawRight
			var cutRight *cutFailoverConn
			if both {
				cutLeft.limit = 0
				cutRight = &cutFailoverConn{Conn: rawRight, cut: make(chan struct{})}
				rightConn = cutRight
			}
			left := NewFailoverConn(context.Background(), cutLeft, nil, nil)
			right := NewFailoverConn(context.Background(), rightConn, nil, nil)
			defer left.Close()
			defer right.Close()
			payload := bytes.Repeat([]byte("native-failover-frame"), 7000)
			results := make(chan error, 4)
			write := func(conn net.Conn) {
				n, err := conn.Write(payload)
				if err == nil && n != len(payload) {
					err = io.ErrShortWrite
				}
				results <- err
			}
			read := func(conn net.Conn) {
				var received bytes.Buffer
				buffer := make([]byte, 257)
				for received.Len() < len(payload) {
					n, err := conn.Read(buffer)
					_, _ = received.Write(buffer[:n])
					if err != nil {
						results <- err
						return
					}
				}
				if !bytes.Equal(received.Bytes(), payload) {
					results <- SessionBroken
					return
				}
				results <- nil
			}
			go write(left)
			if both {
				go write(right)
				failoverMustFinish(t, cutRight.cut)
			}
			go read(right)
			if both {
				go read(left)
			}
			failoverMustFinish(t, cutLeft.cut)
			replacementLeft, replacementRight := net.Pipe()
			restored := make(chan error, 2)
			go func() { restored <- left.RestoreConn(&shortFailoverConn{replacementLeft}) }()
			go func() { restored <- right.RestoreConn(replacementRight) }()
			for range 2 {
				if err := <-restored; err != nil {
					t.Fatal("restore:", err)
				}
			}
			count := 2
			if both {
				count = 4
			}
			for range count {
				if err := <-results; err != nil {
					t.Fatal("resumed stream:", err)
				}
			}
			frames := uint32((len(payload) + 65534) / 65535)
			if right.readIndex != frames || (both && left.readIndex != frames) {
				t.Fatal("resume acknowledgment counts application reads instead of complete wire frames")
			}
		})
	}
}
