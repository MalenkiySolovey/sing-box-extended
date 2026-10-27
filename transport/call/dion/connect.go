package dion

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/transport/call/common"
	"github.com/sagernet/sing-box/transport/call/tunnel"
	"github.com/sagernet/sing-box/transport/call/tunnel/rtc"
	"github.com/sagernet/sing/common/logger"
	N "github.com/sagernet/sing/common/network"
)

func ConnectCreator(ctx context.Context, cookieStr, roomID, email, password string, readBuf int, dialer N.Dialer, dnsRouter adapter.DNSRouter, logger logger.ContextLogger) (*tunnel.RelayBridge, string, error) {
	ctx, cancel := context.WithCancel(ctx)
	retained := false
	defer func() {
		if !retained {
			cancel()
		}
	}()
	auth, err := NewSession(dialer)
	if err != nil {
		return nil, "", fmt.Errorf("dion: new session: %w", err)
	}
	if err := auth.LoadCookieString(cookieStr); err != nil {
		return nil, "", fmt.Errorf("dion: load cookies: %w", err)
	}
	auth.SetCredentials(email, password)
	if err := auth.EnsureValidToken(); err != nil {
		return nil, "", fmt.Errorf("dion: ensure valid token: %w", err)
	}
	requestedRoom := ParseRoom(roomID)
	var event *EventInfo
	if requestedRoom != "" {
		event, err = auth.GetEventBySlug(requestedRoom)
		if err != nil {
			return nil, "", fmt.Errorf("dion: get event by slug: %w", err)
		}
	} else {
		event, err = auth.CreateRoom()
		if err != nil {
			return nil, "", fmt.Errorf("dion: create room: %w", err)
		}
	}
	joinLink := WebBase + "/event/" + event.Slug
	if readBuf <= 0 {
		readBuf = 32768
	}
	obf, err := tunnel.NewTunnelObfuscator(tunnel.DeriveSecretFromJoinLink(event.Slug))
	if err != nil {
		return nil, "", fmt.Errorf("dion: obfuscator init: %w", err)
	}
	relayCh := make(chan *tunnel.RelayBridge, 1)
	var activeRelay *tunnel.RelayBridge
	var relayMu sync.Mutex
	call := NewCall(CallConfig{
		Auth:        auth,
		Event:       event,
		Obfuscator:  obf,
		DisplayName: "Creator",
		Logger:      logger,
		Dialer:      dialer,
		DNSRouter:   dnsRouter,
		Role:        RoleCreator,
	})
	call.OnConnected = func(tun tunnel.DataTunnel) {
		bridgeReadBuf := common.VP8BufSize
		if _, ok := tun.(*rtc.DCTunnel); ok {
			bridgeReadBuf = readBuf
		}
		relayMu.Lock()
		if ctx.Err() != nil {
			relayMu.Unlock()
			return
		}
		active := activeRelay
		if active == nil {
			active = tunnel.NewRelayBridge(tun, "creator", bridgeReadBuf, dialer, logger)
			activeRelay = active
			relayMu.Unlock()
		} else {
			relayMu.Unlock()
			active.SwapTunnel(tun)
		}
		active.MarkReady()
		select {
		case relayCh <- active:
		default:
		}
	}
	call.OnPeerRestart = func() {
		relayMu.Lock()
		active := activeRelay
		relayMu.Unlock()
		if active != nil && ctx.Err() == nil {
			active.Reset()
		}
	}
	done := make(chan struct{})
	startErr := make(chan error, 1)
	go func() {
		defer close(done)
		if err := call.Start(); err != nil {
			startErr <- err
		}
	}()
	stop := func() {
		cancel()
		common.RetireServingTransports(dialer)
		<-done
		call.closeAndWait()
		relayMu.Lock()
		active := activeRelay
		relayMu.Unlock()
		if active != nil {
			active.Close()
		}
	}
	select {
	case relay := <-relayCh:
		relay.SetOwnerCleanup(stop)
		retained = true
		return relay, joinLink, nil
	case err := <-startErr:
		stop()
		return nil, "", fmt.Errorf("dion: call start: %w", err)
	case <-ctx.Done():
		stop()
		return nil, "", ctx.Err()
	case <-time.After(60 * time.Second):
		stop()
		return nil, "", fmt.Errorf("dion: creator tunnel timed out")
	}
}

func ConnectJoiner(ctx context.Context, roomID, displayName string, readBuf int, dialer N.Dialer, dnsRouter adapter.DNSRouter, logger logger.ContextLogger) (*tunnel.RelayBridge, error) {
	if displayName == "" {
		displayName = "Joiner"
	}
	if readBuf <= 0 {
		readBuf = 32768
	}
	joiner := NewDionJoiner(logger, dialer, dnsRouter)
	owner := tunnel.NewJoinerBridge(ctx, dialer, logger, func(tun tunnel.DataTunnel) int { return readBuf }, joiner.MarkConfigAcked)
	joiner.OnConnected = owner.OnConnected
	params := fmt.Sprintf(`{"roomId":%q,"displayName":%q}`, roomID, displayName)
	return owner.Run(func() {
		defer joiner.Close()
		joiner.RunWithParams(params)
	}, joiner.stop)
}
