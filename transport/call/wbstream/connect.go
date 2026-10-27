package wbstream

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/transport/call/common"
	"github.com/sagernet/sing-box/transport/call/tunnel"
	"github.com/sagernet/sing-box/transport/call/tunnel/rtc"
	"github.com/sagernet/sing/common/logger"
	N "github.com/sagernet/sing/common/network"
)

func ConnectCreator(ctx context.Context, cookieStr, roomID, mode string, readBuf int, dialer N.Dialer, logger logger.ContextLogger) (*tunnel.RelayBridge, string, error) {
	ctx, cancel := context.WithCancel(ctx)
	retained := false
	defer func() {
		if !retained {
			cancel()
		}
	}()
	deviceID := common.CookieValue(cookieStr, "__wb_device_id")
	if deviceID == "" {
		return nil, "", fmt.Errorf("wbstream: cookies missing __wb_device_id")
	}
	cookieHeader := common.FilterCookies(cookieStr, WBStreamCookieAllowlist)
	httpClient := common.HttpClient(dialer)
	bearer, rotated, err := RefreshAccessToken(httpClient, cookieHeader, deviceID)
	if err != nil {
		return nil, "", fmt.Errorf("wbstream: slide-v3 refresh: %w", err)
	}
	cookieHeader = mergeCookies(cookieHeader, rotated)
	requestedRoom := ParseRoomID(roomID)
	resolvedRoomID, roomToken, accessToken, serverURL, err := AuthAsLoggedIn(httpClient, cookieHeader, bearer, requestedRoom, "Creator")
	if err != nil {
		return nil, "", fmt.Errorf("wbstream: auth: %w", err)
	}
	if readBuf <= 0 {
		readBuf = 32768
	}
	if mode == "" {
		mode = TunnelModeDC
	}
	obf, err := tunnel.NewTunnelObfuscator(tunnel.DeriveSecretFromJoinLink(resolvedRoomID))
	if err != nil {
		return nil, "", fmt.Errorf("wbstream: obfuscator init: %w", err)
	}

	var relay *tunnel.RelayBridge
	var relayMu sync.Mutex
	joinSession := func(token, access, server string) (*Session, <-chan tunnel.DataTunnel) {
		tunCh := make(chan tunnel.DataTunnel, 1)
		sess := NewSession(SessionConfig{
			RoomToken:   token,
			ServerURL:   server,
			DisplayName: "Creator",
			TunnelMode:  mode,
			Obfuscator:  obf,
			Logger:      logger,
			Dialer:      dialer,
			RoomID:      resolvedRoomID,
			AccessToken: access,
			ReadBuf:     readBuf,
		})
		sess.OnConnected = func(tun tunnel.DataTunnel) {
			select {
			case tunCh <- tun:
			default:
			}
		}
		sess.OnPeerRestart = func() {
			relayMu.Lock()
			active := relay
			relayMu.Unlock()
			if active != nil && ctx.Err() == nil {
				active.Reset()
			}
		}
		return sess, tunCh
	}

	sess, tunCh := joinSession(roomToken, accessToken, serverURL)
	if err := sess.Start(); err != nil {
		sess.Close()
		return nil, "", fmt.Errorf("wbstream: session start: %w", err)
	}
	var firstTun tunnel.DataTunnel
	select {
	case firstTun = <-tunCh:
	case <-ctx.Done():
		common.RetireServingTransports(dialer)
		sess.closeAndWait()
		return nil, "", ctx.Err()
	case <-time.After(60 * time.Second):
		common.RetireServingTransports(dialer)
		sess.closeAndWait()
		return nil, "", fmt.Errorf("wbstream: creator tunnel timed out")
	}

	active := tunnel.NewRelayBridge(firstTun, "creator", bridgeReadBufFor(firstTun, readBuf), dialer, logger)
	relayMu.Lock()
	relay = active
	relayMu.Unlock()
	done := make(chan struct{})
	active.SetOwnerCleanup(func() { cancel(); common.RetireServingTransports(dialer); <-done })
	go func() {
		defer close(done)
		creatorReconnectLoop(ctx, active, sess, joinSession, httpClient, cookieHeader, bearer, deviceID, resolvedRoomID, readBuf, logger)
	}()
	retained = true
	return active, APIBase + "/room/" + resolvedRoomID, nil
}

func ConnectJoiner(ctx context.Context, roomID, displayName, mode string, readBuf int, dialer N.Dialer, dnsRouter adapter.DNSRouter, logger logger.ContextLogger) (*tunnel.RelayBridge, error) {
	roomID = ParseRoomID(roomID)
	if displayName == "" {
		displayName = "Joiner"
	}
	if mode == "" {
		mode = TunnelModeDC
	}
	if readBuf <= 0 {
		readBuf = 32768
	}
	joiner := NewWBStreamJoiner(logger, dialer, dnsRouter, nil)
	owner := tunnel.NewJoinerBridge(ctx, dialer, logger, func(tun tunnel.DataTunnel) int { return bridgeReadBufFor(tun, readBuf) }, joiner.MarkConfigAcked)
	joiner.OnConnected = owner.OnConnected
	params := fmt.Sprintf(`{"roomId":%q,"displayName":%q,"tunnelMode":%q}`, roomID, displayName, mode)
	return owner.Run(func() {
		defer joiner.Close()
		joiner.RunWithParams(params)
	}, joiner.stop)
}

func creatorReconnectLoop(
	ctx context.Context,
	relay *tunnel.RelayBridge,
	sess *Session,
	joinSession func(token, access, server string) (*Session, <-chan tunnel.DataTunnel),
	httpClient *http.Client,
	cookieHeader, bearer, deviceID, roomID string,
	readBuf int,
	logger logger.ContextLogger,
) {
	current := sess
	defer func() { current.closeAndWait() }()
	for {
		select {
		case <-current.Done():
		case <-ctx.Done():
			return
		}
		current.closeAndWait()
		if relay.IsClosed() {
			return
		}
		logger.Debug("wbstream: creator session ended, rejoining")

		var newTun tunnel.DataTunnel
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
			if relay.IsClosed() {
				return
			}
			_, roomToken, accessToken, serverURL, err := AuthAsLoggedIn(httpClient, cookieHeader, bearer, roomID, "Creator")
			if err != nil {
				logger.Warn(fmt.Sprintf("wbstream: rejoin auth failed: %v, refreshing bearer", common.MaskError(err)))
				var rotated map[string]string
				bearer, rotated, err = RefreshAccessToken(httpClient, cookieHeader, deviceID)
				if err != nil {
					logger.Warn(fmt.Sprintf("wbstream: rejoin token refresh failed: %v, retrying", common.MaskError(err)))
					continue
				}
				cookieHeader = mergeCookies(cookieHeader, rotated)
				_, roomToken, accessToken, serverURL, err = AuthAsLoggedIn(httpClient, cookieHeader, bearer, roomID, "Creator")
				if err != nil {
					logger.Warn(fmt.Sprintf("wbstream: rejoin auth failed: %v, retrying", common.MaskError(err)))
					continue
				}
			}
			newSess, tunCh := joinSession(roomToken, accessToken, serverURL)
			if err := newSess.Start(); err != nil {
				newSess.Close()
				logger.Warn(fmt.Sprintf("wbstream: rejoin session start failed: %v, retrying", common.MaskError(err)))
				continue
			}
			select {
			case newTun = <-tunCh:
			case <-ctx.Done():
				newSess.closeAndWait()
				return
			case <-time.After(60 * time.Second):
				logger.Warn("wbstream: rejoin tunnel timed out, retrying")
				newSess.closeAndWait()
				continue
			}
			current = newSess
			break
		}
		relay.SwapTunnel(newTun)
		logger.Info(fmt.Sprintf("wbstream: creator tunnel reconnected, buf=%d", bridgeReadBufFor(newTun, readBuf)))
	}
}

func bridgeReadBufFor(tun tunnel.DataTunnel, readBuf int) int {
	switch tun.(type) {
	case *rtc.DCTunnel, *rtc.MultiTrackKCPTunnel:
		return readBuf
	}
	return common.VP8BufSize
}
