package tunnel

import (
	"context"
	"net"
	"sync"

	"github.com/sagernet/sing-box/transport/call/common"
	"github.com/sagernet/sing/common/logger"
	N "github.com/sagernet/sing/common/network"
)

// JoinerBridge owns one platform worker and its reconnecting relay. Platforms
// retain ownership of their peers; stop interrupts signaling, and run must
// release the peers before returning. A late callback cannot publish a relay.
type JoinerBridge struct {
	ctx       context.Context
	dialer    N.Dialer
	logger    logger.ContextLogger
	readBuf   func(DataTunnel) int
	configAck func()

	mu          sync.Mutex
	started     bool
	retired     bool
	bridge      *RelayBridge
	ready       chan struct{}
	done        chan struct{}
	stop        func()
	stopContext func() bool
	stopOnce    sync.Once
}

func NewJoinerBridge(ctx context.Context, dialer N.Dialer, logger logger.ContextLogger, readBuf func(DataTunnel) int, configAck func()) *JoinerBridge {
	return &JoinerBridge{ctx: ctx, dialer: dialer, logger: logger, readBuf: readBuf, configAck: configAck, ready: make(chan struct{}), done: make(chan struct{})}
}

func (j *JoinerBridge) OnConnected(tun DataTunnel) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.started || j.retired || j.ctx.Err() != nil || tun == nil {
		return
	}
	if j.bridge != nil {
		j.bridge.SwapTunnel(tun)
		return
	}
	bridge := NewRelayBridge(tun, "joiner", j.readBuf(tun), j.dialer, j.logger)
	bridge.SetOnConfigAck(j.configAck)
	bridge.SetOwnerCleanup(j.retire)
	bridge.MarkReady()
	j.bridge = bridge
	close(j.ready)
}

func (j *JoinerBridge) Run(run, stop func()) (*RelayBridge, error) {
	j.mu.Lock()
	if j.started || j.ctx.Err() != nil {
		j.mu.Unlock()
		return nil, net.ErrClosed
	}
	j.started, j.stop = true, stop
	j.stopContext = context.AfterFunc(j.ctx, j.retire)
	j.mu.Unlock()
	go func() {
		run()
		close(j.done)
		j.retire()
	}()
	select {
	case <-j.ready:
		j.mu.Lock()
		bridge, retired := j.bridge, j.retired
		j.mu.Unlock()
		if !retired && j.ctx.Err() == nil && !bridge.IsClosed() {
			return bridge, nil
		}
	case <-j.done:
	case <-j.ctx.Done():
	}
	j.retire()
	if err := j.ctx.Err(); err != nil {
		return nil, err
	}
	return nil, net.ErrClosed
}

func (j *JoinerBridge) retire() {
	j.stopOnce.Do(func() {
		j.mu.Lock()
		j.retired = true
		stopContext := j.stopContext
		j.mu.Unlock()
		stopContext()
		common.RetireServingTransports(j.dialer)
		j.stop()
		<-j.done
	})
	j.mu.Lock()
	bridge := j.bridge
	j.mu.Unlock()
	// Close can call this owner again; the once must already have completed.
	if bridge != nil {
		bridge.Close()
	}
}
