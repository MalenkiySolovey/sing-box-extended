package tunnel

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing/common/logger"
)

type retirementTunnel struct {
	sent chan struct{}
	once sync.Once
}

func (t *retirementTunnel) SendData([]byte)      { t.once.Do(func() { close(t.sent) }) }
func (*retirementTunnel) SetOnData(func([]byte)) {}
func (*retirementTunnel) SetOnClose(func())      {}
func (*retirementTunnel) Reconfigure(int, int)   {}

func TestCallRelayRetiresPendingAndPacketStreams(t *testing.T) {
	wire := &retirementTunnel{sent: make(chan struct{})}
	relay := NewRelayBridge(wire, "joiner", 32768, nil, logger.NOP())
	defer relay.Close()
	relay.MarkReady()
	var cleanup atomic.Int32
	relay.SetOwnerCleanup(func() { cleanup.Add(1) })
	packet, err := relay.ListenPacket(t.Context(), "192.0.2.1:53")
	if err != nil {
		t.Fatal(err)
	}
	read := make(chan error, 1)
	go func() { _, err := packet.Read(make([]byte, 64)); read <- err }()
	dial := make(chan error, 1)
	go func() { _, err := relay.DialContext(t.Context(), "192.0.2.1:443"); dial <- err }()
	select {
	case <-wire.sent:
	case <-time.After(time.Second):
		t.Fatal("relay dial did not start")
	}
	relay.Close()
	relay.Close()
	for _, done := range []<-chan error{read, dial} {
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("retired relay returned a live operation")
			}
		case <-time.After(time.Second):
			t.Fatal("relay retirement left a blocked operation")
		}
	}
	if cleanup.Load() != 1 {
		t.Fatal("platform cleanup was not owned exactly once")
	}
	relay.SetOwnerCleanup(func() { cleanup.Add(1) })
	if cleanup.Load() != 2 {
		t.Fatal("late platform owner escaped retirement")
	}
	if _, err := relay.DialContext(context.Background(), "192.0.2.1:443"); err == nil {
		t.Fatal("retired relay admitted another stream")
	}

	creator := NewRelayBridge(&retirementTunnel{sent: make(chan struct{})}, "creator", 32768, nil, logger.NOP())
	udp := newCreatorUDPConn(7, creator, "192.0.2.2:53")
	creator.udpClients.Store(uint32(7), udp)
	creator.Close()
	if _, err := udp.Read(make([]byte, 64)); !errors.Is(err, io.EOF) {
		t.Fatal("creator packet stream survived relay retirement")
	}
	if _, _, err := creator.retain(&creator.udpClients, 8, udp); !errors.Is(err, net.ErrClosed) {
		t.Fatal("retired creator accepted a late routed stream")
	}
}

func TestCallCreatorPreservesUDPDatagramBoundaries(t *testing.T) {
	relay := NewRelayBridge(&retirementTunnel{sent: make(chan struct{})}, "creator", 32768, nil, logger.NOP())
	defer relay.Close()
	conn := newCreatorUDPConn(1, relay, "192.0.2.1:53")
	defer conn.Close()
	conn.deliver([]byte("first"))
	conn.deliver([]byte("second-packet"))
	for _, expected := range []string{"first", "second-packet"} {
		buffer := make([]byte, 64)
		n, err := conn.Read(buffer)
		if err != nil || string(buffer[:n]) != expected {
			t.Fatal("Call UDP packets were concatenated or split")
		}
	}
}
