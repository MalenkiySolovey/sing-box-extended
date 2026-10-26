package multiplex

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// TestSession_LargeTransferBackpressure verifies that a transfer larger than
// maxQueuedBytesPerStream completes correctly: the demux loop applies
// backpressure (cond.Wait) instead of dropping data, and the reader draining
// the stream wakes the blocked loop without deadlock.
func TestSession_LargeTransferBackpressure(t *testing.T) {
	c1, c2 := net.Pipe()

	client, err := NewClientSession(c1)
	if err != nil {
		t.Fatalf("client session: %v", err)
	}
	server, err := NewServerSession(c2)
	if err != nil {
		t.Fatalf("server session: %v", err)
	}
	defer client.Close()
	defer server.Close()

	// Payload bigger than the per-stream backpressure window (4MB).
	const total = 12 * 1024 * 1024
	payload := make([]byte, total)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	var writeErr error
	go func() {
		defer wg.Done()
		stream, err := client.OpenStream([]byte("hello"))
		if err != nil {
			writeErr = err
			return
		}
		defer stream.Close()
		if _, err := stream.Write(payload); err != nil {
			writeErr = err
			return
		}
		_ = stream.(interface{ CloseWrite() error }).CloseWrite()
	}()

	var got []byte
	var readErr error
	go func() {
		defer wg.Done()
		stream, openPayload, err := server.AcceptStream()
		if err != nil {
			readErr = err
			return
		}
		if string(openPayload) != "hello" {
			readErr = io.ErrUnexpectedEOF
			return
		}
		got, readErr = io.ReadAll(stream)
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("transfer deadlocked (backpressure did not release)")
	}

	if writeErr != nil {
		t.Fatalf("write: %v", writeErr)
	}
	if readErr != nil {
		t.Fatalf("read: %v", readErr)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

func fillReceiveWindow(t *testing.T, st *stream) {
	t.Helper()
	for n := 0; n < maxQueuedBytesPerStream; n += maxDataPayload {
		if err := st.enqueue(make([]byte, maxDataPayload)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStreamBusyReaderCanReleaseReceiveWindow(t *testing.T) {
	st := newStream(nil, 1)
	defer st.Close()
	fillReceiveWindow(t, st)
	queued := make(chan error, 1)
	go func() { queued <- st.enqueue([]byte("next-frame")) }()
	// A temporarily descheduled reader must not lose an otherwise valid stream.
	select {
	case err := <-queued:
		t.Fatal("receive window reset before the active reader could drain it", err)
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := st.Read(make([]byte, maxDataPayload)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-queued:
		if err != nil {
			t.Fatal("reader did not release backpressure", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reader did not wake queued frame")
	}
	if st.queuedBytes > maxQueuedBytesPerStream {
		t.Fatal("reader grace exceeded the receive memory bound")
	}
}

func TestStreamStalledReaderKeepsReceiveWaitBounded(t *testing.T) {
	st := newStream(nil, 1)
	defer st.Close()
	fillReceiveWindow(t, st)
	queued := make(chan error, 1)
	go func() { queued <- st.enqueue([]byte("next-frame")) }()
	select {
	case err := <-queued:
		if !errors.Is(err, errMuxReceiveQueueFull) {
			t.Fatal("stalled reader did not retain bounded reset", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stalled reader indefinitely blocked the shared demultiplexer")
	}
	if st.queuedBytes != maxQueuedBytesPerStream {
		t.Fatal("stalled receive window grew")
	}
}

func TestStreamCloseReleasesPendingReceiveWait(t *testing.T) {
	st := newStream(nil, 1)
	fillReceiveWindow(t, st)
	queued := make(chan error, 1)
	go func() { queued <- st.enqueue([]byte("next-frame")) }()
	select {
	case err := <-queued:
		t.Fatal("receive window reset before close could retire its pending frame", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-queued:
		if err != nil {
			t.Fatal("close did not retire pending frame", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not wake pending receive")
	}
	if st.queuedBytes != 0 {
		t.Fatal("close admitted data after retirement")
	}
}
