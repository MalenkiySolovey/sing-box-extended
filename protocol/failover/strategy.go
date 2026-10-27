package failover

import (
	"context"
	"net"
	"time"

	"github.com/sagernet/sing-box/adapter"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
)

type DialStrategy = func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error)

func cycleStrategy(outbounds []adapter.Outbound, logger logger.ContextLogger, delay time.Duration) DialStrategy {
	return func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
		for {
			for _, outbound := range outbounds {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				conn, err := outbound.DialContext(ctx, network, destination)
				if err != nil {
					logger.InfoContext(ctx, err)
					if delay > 0 {
						select {
						case <-time.After(delay):
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					continue
				}
				if err := ctx.Err(); err != nil {
					_ = conn.Close()
					return nil, err
				}
				return conn, nil
			}
		}
	}
}

func sequentialStrategy(outbounds []adapter.Outbound, logger logger.ContextLogger, delay time.Duration) DialStrategy {
	return func(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
		var err error
		for _, outbound := range outbounds {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			var conn net.Conn
			conn, err = outbound.DialContext(ctx, network, destination)
			if err != nil {
				logger.InfoContext(ctx, err)
				if delay > 0 {
					select {
					case <-time.After(delay):
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				continue
			}
			if err := ctx.Err(); err != nil {
				_ = conn.Close()
				return nil, err
			}
			return conn, nil
		}
		return nil, err
	}
}

func CreateStrategy(strategy string, outbounds []adapter.Outbound, logger logger.ContextLogger, delay time.Duration) (DialStrategy, error) {
	if len(outbounds) == 0 {
		return nil, E.New("missing outbounds")
	}
	switch strategy {
	case "cycle":
		return cycleStrategy(outbounds, logger, delay), nil
	case "sequential", "":
		return sequentialStrategy(outbounds, logger, delay), nil
	default:
		return nil, E.New("strategy not found: ", strategy)
	}
}
