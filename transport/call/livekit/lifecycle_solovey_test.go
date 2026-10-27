package livekit

import (
	"testing"
	"time"

	"github.com/sagernet/sing/common/logger"
)

func TestCallLivekitRetirementWakesSignalingWorkers(t *testing.T) {
	for _, joined := range []bool{false, true} {
		client, err := NewClient(Config{Codec: ProtoCodec{}, Logger: logger.NOP()})
		if err != nil {
			t.Fatal(err)
		}
		if joined {
			client.join.PingIntervalSec = 3600
			close(client.joined)
		}
		done := make(chan struct{})
		go func() { client.PingLoop(); close(done) }()
		client.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("retired signaling retained its ping worker")
		}
		if client.ctx.Err() == nil {
			t.Fatal("retirement retained signaling dial context")
		}
		if err := client.Connect(); err == nil {
			t.Fatal("closed client admitted a signaling connection")
		}
	}
	client, _ := NewClient(Config{Codec: ProtoCodec{}, Logger: logger.NOP()})
	if err := client.Run(); err == nil {
		t.Fatal("signaling without a connection unexpectedly ran")
	}
	if !client.closed.Load() {
		t.Fatal("read worker exit did not retire its ping worker")
	}
}
