package bond

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

type bufferConn struct {
	bytes.Buffer
	maxWrite int
}

func (c *bufferConn) Read(p []byte) (int, error) { return c.Buffer.Read(p) }
func (c *bufferConn) Write(p []byte) (int, error) {
	if c.maxWrite > 0 && len(p) > c.maxWrite {
		p = p[:c.maxWrite]
	}
	return c.Buffer.Write(p)
}
func (*bufferConn) Close() error                     { return nil }
func (*bufferConn) LocalAddr() net.Addr              { return nil }
func (*bufferConn) RemoteAddr() net.Addr             { return nil }
func (*bufferConn) SetDeadline(time.Time) error      { return nil }
func (*bufferConn) SetReadDeadline(time.Time) error  { return nil }
func (*bufferConn) SetWriteDeadline(time.Time) error { return nil }

func TestBondFramesPreserveLargeAndShortWrites(t *testing.T) {
	for _, maxWrite := range []int{0, 1} {
		t.Run(map[bool]string{true: "short-writer", false: "normal-writer"}[maxWrite == 1], func(t *testing.T) {
			legs := []net.Conn{&bufferConn{maxWrite: maxWrite}, &bufferConn{maxWrite: maxWrite}, &bufferConn{maxWrite: maxWrite}}
			ratios := []uint8{33, 0, 67}
			writer := NewBondedConn(legs, ratios, ratios)
			payload := bytes.Repeat([]byte("bounded-bond-frame\x00"), 7000)
			if n, err := writer.Write(payload); err != nil || n != len(payload) {
				t.Fatalf("write: %d of %d, %v", n, len(payload), err)
			}
			reader := NewBondedConn(legs, ratios, ratios)
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(reader, got); err != nil {
				t.Fatal("read large multi-frame stream:", err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatal("Bond framing corrupted a write larger than its 16-bit frame length")
			}
		})
	}
}

func TestBondReadNeverCountsBytesNotCopiedToCaller(t *testing.T) {
	// Header promises four bytes, but the peer closes after only two.
	leg := &bufferConn{}
	leg.Buffer.Write([]byte{0, 4, 'a', 'b'})
	conn := NewBondedConn([]net.Conn{leg}, []uint8{100}, []uint8{100})
	p := []byte{0xff}
	n, err := conn.Read(p)
	if n > len(p) || (n > 0 && p[0] != 'a') || (n == 0 && err == nil) {
		t.Fatal("Read reported internal buffered bytes as copied caller bytes")
	}
}
