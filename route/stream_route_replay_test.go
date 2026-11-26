package route

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing/common/buf"
)

func TestGuardedStreamReplayKeepsNativePrefixOrder(t *testing.T) {
	local, peer := net.Pipe()
	defer peer.Close()
	if wrapGuardedStreamReplay(local, nil) != local {
		t.Fatal("unexpected wrapper for a stream without sniff bytes")
	}
	conn := wrapGuardedStreamReplay(local, []*buf.Buffer{guardTestBuffer("middle"), guardTestBuffer("first")}).(*guardedStreamReplay)
	defer conn.Close()
	if conn.ReaderReplaceable() {
		t.Fatal("reader exposed while prefix is pending")
	}
	first := conn.ReadCached()
	if first == nil {
		t.Fatal("early handshake did not receive the outermost buffer")
	}
	defer first.Release()
	if got := string(first.Bytes()); got != "first" {
		t.Fatalf("early handshake got %q, want first", got)
	}
	partial := make([]byte, 3)
	if _, err := io.ReadFull(conn, partial); err != nil || string(partial) != "mid" || conn.ReaderReplaceable() {
		t.Fatalf("partial replay: %q, %v", partial, err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	go func() {
		defer peer.Close()
		_, _ = io.WriteString(peer, "tail")
	}()
	if remaining, err := io.ReadAll(conn); err != nil || string(remaining) != "dletail" || !conn.ReaderReplaceable() {
		t.Fatalf("remaining replay: %q, %v", remaining, err)
	}
	_ = conn.Close()
	if got := string(first.Bytes()); got != "first" {
		t.Fatalf("connection closed a buffer already owned by the handshake: %q", got)
	}
}

func TestGuardedStreamReplayConcurrentClose(t *testing.T) {
	for range 32 {
		local, peer := net.Pipe()
		conn := wrapGuardedStreamReplay(local, []*buf.Buffer{guardTestBuffer("head")})
		start := make(chan struct{})
		read := make(chan bool, 1)
		closed := make(chan struct{})
		go func() {
			<-start
			data := make([]byte, 5)
			n, err := io.ReadFull(conn, data)
			read <- err != nil && (n == 0 || string(data[:n]) == "head")
		}()
		go func() {
			<-start
			_ = conn.Close()
			close(closed)
		}()
		close(start)
		select {
		case valid := <-read:
			_ = peer.Close()
			<-closed
			if !valid {
				t.Fatal("close corrupted replay or did not report a short stream")
			}
		case <-time.After(time.Second):
			_ = peer.Close()
			t.Fatal("close did not interrupt the reader")
		}
	}
}

func guardTestBuffer(text string) *buf.Buffer {
	buffer := buf.New()
	copy(buffer.Extend(len(text)), text)
	return buffer
}
