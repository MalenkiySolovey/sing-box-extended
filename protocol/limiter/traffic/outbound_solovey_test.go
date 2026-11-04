package traffic

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
)

func TestTrafficConfiguredPoliciesOwnIndependentBudgets(t *testing.T) {
	var options option.TrafficLimiterOutboundOptions
	if err := json.Unmarshal([]byte(`{"strategy":"users","users":[{"name":"alice","strategy":"global","mode":"bidirectional","total":6},{"name":"bob","strategy":"bypass"}]}`), &options); err != nil {
		t.Fatal(err)
	}
	strategy, err := configuredStrategy(options)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	selected, _, err := strategy.(*ManagerTrafficStrategy).getStrategy(ctx, &adapter.InboundContext{User: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	raw := &partialTrafficConn{}
	conn, err := selected.wrapConn(ctx, raw, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := conn.Write(make([]byte, 3)); n != 3 || err != nil {
		t.Fatalf("write = %d, %v", n, err)
	}
	if n, err := conn.Read(make([]byte, 3)); n != 3 || err != nil {
		t.Fatalf("read = %d, %v", n, err)
	}
	if n, err := conn.Write([]byte{1}); n != 0 || !errors.Is(err, errTrafficLimitExceeded) {
		t.Fatalf("exhausted budget = %d, %v", n, err)
	}
	if _, err := strategy.wrapConn(ctx, raw, &adapter.InboundContext{User: "bob"}, false); err != nil {
		t.Fatal("bypass user inherited another user's exhausted budget", err)
	}
	if _, err := strategy.wrapConn(ctx, raw, &adapter.InboundContext{User: "absent"}, false); err == nil {
		t.Fatal("unknown user acquired a policy")
	}
	options.Strategy, options.Mode = "global", "download"
	if err := json.Unmarshal([]byte("3"), &options.Total); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		fresh, err := configuredStrategy(options)
		if err != nil {
			t.Fatal(err)
		}
		limiter, err := fresh.(*DefaultWrapStrategy).limiterStrategy.getLimiter(ctx, nil)
		if err != nil || limiter.Add(3) != nil || limiter.Can(1) == nil {
			t.Fatal("configured instance did not own its fresh budget", err)
		}
	}
}

func TestTrafficPartialIOPreservesTransferredBytesAndAddress(t *testing.T) {
	ctx := context.Background()
	for _, ioErr := range []error{nil, io.EOF} {
		limiter := &staticTrafficLimiter{total: 100}
		conn := newConnWithUploadTrafficLimiter(ctx, newConnWithDownloadTrafficLimiter(ctx, &partialTrafficConn{err: ioErr}, limiter), limiter)
		if n, err := conn.Read(make([]byte, 9)); n != 3 || !errors.Is(err, ioErr) {
			t.Fatalf("partial read = %d, %v", n, err)
		}
		if n, err := conn.Write(make([]byte, 9)); n != 3 || !errors.Is(err, ioErr) {
			t.Fatalf("partial write = %d, %v", n, err)
		}
		packet := newPacketConnWithUploadTrafficLimiter(ctx, newPacketConnWithDownloadTrafficLimiter(ctx, &partialTrafficPacketConn{err: ioErr}, limiter), limiter)
		addr := &net.UDPAddr{Port: 7}
		if n, err := packet.WriteTo(make([]byte, 9), addr); n != 3 || !errors.Is(err, ioErr) {
			t.Fatalf("partial packet write = %d, %v", n, err)
		}
		if n, from, err := packet.ReadFrom(make([]byte, 9)); n != 3 || from.String() != addr.String() || !errors.Is(err, ioErr) {
			t.Fatalf("partial packet read = %d, %v, %v", n, from, err)
		}
		if limiter.used != 12 {
			t.Fatalf("transferred bytes = %d, want 12", limiter.used)
		}
	}
	limiter := &staticTrafficLimiter{total: 1}
	if n, err := newConnWithUploadTrafficLimiter(ctx, &partialTrafficConn{}, limiter).Read(make([]byte, 3)); n != 3 || !errors.Is(err, errTrafficLimitExceeded) {
		t.Fatalf("quota error lost already-read bytes: %d, %v", n, err)
	}
}

func TestTrafficBudgetRejectsOverflowAndInvalidPolicy(t *testing.T) {
	limiter := &staticTrafficLimiter{total: math.MaxUint64, used: math.MaxUint64 - 1}
	if limiter.Add(2) == nil || limiter.used != math.MaxUint64 || limiter.Can(1) == nil {
		t.Fatal("counter overflow restored an exhausted budget")
	}
	for _, raw := range []string{`{}`, `{"strategy":"global","mode":"download"}`, `{"strategy":"users","users":[{"name":"","strategy":"bypass"}]}`, `{"strategy":"users","users":[{"name":"x","strategy":"bypass"},{"name":"x","strategy":"bypass"}]}`} {
		var options option.TrafficLimiterOutboundOptions
		if err := json.Unmarshal([]byte(raw), &options); err != nil {
			t.Fatal(err)
		}
		if _, err := configuredStrategy(options); err == nil {
			t.Fatal("invalid traffic policy accepted")
		}
	}
}

type partialTrafficConn struct {
	net.Conn
	err error
}

func (c *partialTrafficConn) Read([]byte) (int, error)  { return 3, c.err }
func (c *partialTrafficConn) Write([]byte) (int, error) { return 3, c.err }

type partialTrafficPacketConn struct {
	net.PacketConn
	err error
}

func (c *partialTrafficPacketConn) ReadFrom([]byte) (int, net.Addr, error) {
	return 3, &net.UDPAddr{Port: 7}, c.err
}
func (c *partialTrafficPacketConn) WriteTo([]byte, net.Addr) (int, error) { return 3, c.err }
