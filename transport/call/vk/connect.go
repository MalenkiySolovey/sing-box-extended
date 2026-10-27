package vk

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/transport/call/common"
	"github.com/sagernet/sing-box/transport/call/tunnel"
	"github.com/sagernet/sing-box/transport/call/tunnel/rtc"
	"github.com/sagernet/sing/common/logger"
	N "github.com/sagernet/sing/common/network"
)

func ConnectCreator(ctx context.Context, cookieStr, joinLink string, readBuf int, dialer N.Dialer, dnsRouter adapter.DNSRouter, logger logger.ContextLogger) (*tunnel.RelayBridge, string, error) {
	ctx, cancel := context.WithCancel(ctx)
	retained := false
	defer func() {
		if !retained {
			cancel()
		}
	}()
	cfg, err := FetchConfig(logger)
	if err != nil {
		return nil, "", err
	}
	var callInfo *CallInfo
	if joinLink != "" {
		callInfo, err = JoinExistingCall(dialer, cookieStr, joinLink, cfg, logger)
	} else {
		callInfo, err = CreateAndJoinCall(dialer, cookieStr, "", cfg, logger)
	}
	if err != nil {
		return nil, "", err
	}
	if readBuf <= 0 {
		readBuf = 32768
	}
	obf, err := tunnel.NewTunnelObfuscator(tunnel.DeriveSecretFromJoinLink(callInfo.JoinLink))
	if err != nil {
		return nil, "", fmt.Errorf("vk: obfuscator init: %w", err)
	}
	bridge := &Bridge{
		ctx:       ctx,
		dialer:    dialer,
		dnsRouter: dnsRouter,
		readBuf:   readBuf,
		logger:    logger,
	}
	bridge.newRelay = func() Relay {
		ur := NewTunnelRelay(dialer, logger)
		ur.readBufSize = readBuf
		ur.SetObfuscator(obf)
		ur.OnConnected = func(tun tunnel.DataTunnel) {
			if ctx.Err() != nil {
				return
			}
			bridgeReadBuf := common.VP8BufSize
			if _, ok := tun.(*rtc.DCTunnel); ok {
				bridgeReadBuf = readBuf
			}
			bridge.mu.Lock()
			if ctx.Err() != nil {
				bridge.mu.Unlock()
				return
			}
			rb := bridge.activeBridge
			if rb == nil {
				rb = tunnel.NewRelayBridge(tun, "creator", bridgeReadBuf, dialer, logger)
				bridge.activeBridge = rb
				bridge.mu.Unlock()
			} else {
				bridge.mu.Unlock()
				rb.SwapTunnel(tun)
			}
			rb.MarkReady()
			if st, ok := tun.(*rtc.SymmetricScreenTunnel); ok {
				rb.SetOnPeerConfig(func(fps, batch, trackCount int) {
					st.SetTrackCount(trackCount)
					bridge.setScreenSharing(trackCount > 1)
				})
			}
		}
		return ur
	}
	done := make(chan struct{})
	go func() { defer close(done); bridge.Run(ctx, callInfo, cookieStr, cfg) }()
	stop := func() {
		cancel()
		common.RetireServingTransports(dialer)
		bridge.mu.Lock()
		sfu := bridge.sfu
		bridge.mu.Unlock()
		if sfu != nil {
			_ = sfu.Close()
		}
		<-done
		if bridge.relay != nil {
			bridge.relay.Close()
		}
		bridge.mu.Lock()
		active := bridge.activeBridge
		bridge.mu.Unlock()
		if active != nil {
			active.Close()
		}
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		bridge.mu.Lock()
		rb := bridge.activeBridge
		bridge.mu.Unlock()
		if rb != nil {
			rb.SetOwnerCleanup(stop)
			retained = true
			return rb, callInfo.JoinLink, nil
		}
		if time.Now().After(deadline) {
			stop()
			return nil, "", fmt.Errorf("vk: creator tunnel timed out")
		}
		select {
		case <-ctx.Done():
			stop()
			return nil, "", ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func ConnectJoiner(ctx context.Context, joinLink, displayName string, readBuf int, dialer N.Dialer, dnsRouter adapter.DNSRouter, logger logger.ContextLogger) (*tunnel.RelayBridge, error) {
	if displayName == "" {
		displayName = "Joiner"
	}
	if readBuf <= 0 {
		readBuf = 32768
	}
	authJSON, err := RunVKAuth(dialer, joinLink, displayName, logger)
	if err != nil {
		return nil, fmt.Errorf("vk: auth: %w", err)
	}
	var params VKAuthParams
	if err := json.Unmarshal([]byte(authJSON), &params); err != nil {
		return nil, fmt.Errorf("vk: decode auth params: %w", err)
	}
	params.TunnelMode = "video"
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("vk: encode auth params: %w", err)
	}
	joiner := NewVKJoiner(
		logger,
		nil,
		common.AddTunnelTracks,
		common.ReadTrack,
		dialer,
		dnsRouter,
	)
	tunCh := make(chan tunnel.DataTunnel, 1)
	joiner.OnConnected = func(tun tunnel.DataTunnel) {
		select {
		case tunCh <- tun:
		default:
		}
	}
	go joiner.RunWithParams(string(paramsJSON))
	select {
	case tun := <-tunCh:
		rb := tunnel.NewRelayBridge(tun, "joiner", readBuf, dialer, logger)
		rb.SetOnConfigAck(joiner.MarkConfigAcked)
		rb.MarkReady()
		return rb, nil
	case <-ctx.Done():
		joiner.Close()
		return nil, ctx.Err()
	}
}
