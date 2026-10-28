package v2raykcp

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type stockPacketHandler struct{ received chan []byte }

func (h stockPacketHandler) NewConnectionEx(_ context.Context, conn net.Conn, _, _ M.Socksaddr, _ N.CloseHandlerFunc) {
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	data := make([]byte, 1312)
	if _, err := io.ReadFull(conn, data); err == nil {
		h.received <- data
	}
}

// Pristine Shtorm d9442d14 budgets the header and GCM tag, but not the
// transmitted nonce, against MTU. A valid full stock packet is MTU+12 bytes.
func TestKCPAcceptsStockSeededPacket(t *testing.T) {
	options := option.V2RayKCPOptions{MTU: 1350, Seed: "synthetic-stock-mkcp", HeaderType: "srtp", TTI: 10}
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan []byte, 1)
	server, err := NewServer(t.Context(), log.NewNOPFactory().Logger(), options, nil, stockPacketHandler{received})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.ServePacket(listener) }()
	t.Cleanup(func() {
		_ = listener.Close()
		_ = server.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("stock packet fixture retained its reader")
		}
	})
	config := NewConfig(options)
	security, _ := config.GetSecurity()
	header := config.GetPacketHeader()
	payload := bytes.Repeat([]byte{0x5a}, int(options.MTU)-header.Size()-security.Overhead()-DataSegmentOverhead)
	if len(payload) != 1312 {
		t.Fatal("stock packet fixture no longer fills its original MTU budget")
	}
	segment := NewDataSegment()
	defer segment.Release()
	segment.Conv = 42
	_, _ = segment.Data().Write(payload)
	plain := make([]byte, segment.ByteSize())
	segment.Serialize(plain)
	packet := make([]byte, header.Size()+security.NonceSize())
	header.Serialize(packet[:header.Size()])
	packet = security.Seal(packet, packet[header.Size():], plain, nil)
	if len(packet) != 1362 {
		t.Fatal("fixture did not reproduce the stock nonce overhead")
	}
	peer, err := net.Dial("udp", listener.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if _, err := peer.Write(packet); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-received:
		if !bytes.Equal(got, payload) {
			t.Fatal("stock seeded packet lost stream bytes")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("valid stock MTU+nonce packet was truncated or rejected")
	}
}
