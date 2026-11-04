package traffic

import (
	"context"
	"net"
)

type connWithTrafficLimiter struct {
	net.Conn
	ctx     context.Context
	limiter TrafficLimiter
}

func newConnWithDownloadTrafficLimiter(ctx context.Context, conn net.Conn, limiter TrafficLimiter) net.Conn {
	return &connWithTrafficLimiter{Conn: conn, ctx: ctx, limiter: limiter}
}

func newConnWithUploadTrafficLimiter(ctx context.Context, conn net.Conn, limiter TrafficLimiter) net.Conn {
	return &connWithUploadTrafficLimiter{Conn: conn, ctx: ctx, limiter: limiter}
}

func (conn *connWithTrafficLimiter) Write(p []byte) (int, error) {
	err := conn.limiter.Can(uint64(len(p)))
	if err != nil {
		return 0, err
	}
	n, err := conn.Conn.Write(p)
	accountErr := conn.limiter.Add(uint64(n))
	if err != nil {
		return n, err
	}
	return n, accountErr
}

type connWithUploadTrafficLimiter struct {
	net.Conn
	ctx     context.Context
	limiter TrafficLimiter
}

func (conn *connWithUploadTrafficLimiter) Read(p []byte) (int, error) {
	err := conn.limiter.Can(1)
	if err != nil {
		return 0, err
	}
	n, err := conn.Conn.Read(p)
	accountErr := conn.limiter.Add(uint64(n))
	if err != nil {
		return n, err
	}
	return n, accountErr
}

type packetConnWithTrafficLimiter struct {
	net.PacketConn
	ctx     context.Context
	limiter TrafficLimiter
}

func newPacketConnWithDownloadTrafficLimiter(ctx context.Context, conn net.PacketConn, limiter TrafficLimiter) net.PacketConn {
	return &packetConnWithTrafficLimiter{PacketConn: conn, ctx: ctx, limiter: limiter}
}

func newPacketConnWithUploadTrafficLimiter(ctx context.Context, conn net.PacketConn, limiter TrafficLimiter) net.PacketConn {
	return &packetConnWithUploadTrafficLimiter{PacketConn: conn, ctx: ctx, limiter: limiter}
}

func (conn *packetConnWithTrafficLimiter) WriteTo(p []byte, addr net.Addr) (int, error) {
	err := conn.limiter.Can(uint64(len(p)))
	if err != nil {
		return 0, err
	}
	n, err := conn.PacketConn.WriteTo(p, addr)
	accountErr := conn.limiter.Add(uint64(n))
	if err != nil {
		return n, err
	}
	return n, accountErr
}

type packetConnWithUploadTrafficLimiter struct {
	net.PacketConn
	ctx     context.Context
	limiter TrafficLimiter
}

func (conn *packetConnWithUploadTrafficLimiter) ReadFrom(p []byte) (int, net.Addr, error) {
	err := conn.limiter.Can(1)
	if err != nil {
		return 0, nil, err
	}
	n, addr, err := conn.PacketConn.ReadFrom(p)
	accountErr := conn.limiter.Add(uint64(n))
	if err != nil {
		return n, addr, err
	}
	return n, addr, accountErr
}

func connWithDownloadTrafficWrapper(ctx context.Context, conn net.Conn, limiter TrafficLimiter, reverse bool) net.Conn {
	if reverse {
		return newConnWithUploadTrafficLimiter(ctx, conn, limiter)
	}
	return newConnWithDownloadTrafficLimiter(ctx, conn, limiter)
}

func connWithUploadTrafficWrapper(ctx context.Context, conn net.Conn, limiter TrafficLimiter, reverse bool) net.Conn {
	if reverse {
		return newConnWithDownloadTrafficLimiter(ctx, conn, limiter)
	}
	return newConnWithUploadTrafficLimiter(ctx, conn, limiter)
}

func connWithBidirectionalTrafficWrapper(ctx context.Context, conn net.Conn, limiter TrafficLimiter, reverse bool) net.Conn {
	return newConnWithUploadTrafficLimiter(ctx, newConnWithDownloadTrafficLimiter(ctx, conn, limiter), limiter)
}

func packetConnWithDownloadTrafficWrapper(ctx context.Context, conn net.PacketConn, limiter TrafficLimiter, reverse bool) net.PacketConn {
	if reverse {
		return newPacketConnWithUploadTrafficLimiter(ctx, conn, limiter)
	}
	return newPacketConnWithDownloadTrafficLimiter(ctx, conn, limiter)
}

func packetConnWithUploadTrafficWrapper(ctx context.Context, conn net.PacketConn, limiter TrafficLimiter, reverse bool) net.PacketConn {
	if reverse {
		return newPacketConnWithDownloadTrafficLimiter(ctx, conn, limiter)
	}
	return newPacketConnWithUploadTrafficLimiter(ctx, conn, limiter)
}

func packetConnWithBidirectionalTrafficWrapper(ctx context.Context, conn net.PacketConn, limiter TrafficLimiter, reverse bool) net.PacketConn {
	return newPacketConnWithUploadTrafficLimiter(ctx, newPacketConnWithDownloadTrafficLimiter(ctx, conn, limiter), limiter)
}
