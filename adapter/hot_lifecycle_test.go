package adapter_test

import (
	"context"
	"errors"
	"net"
	"os"
	"reflect"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/adapter/inbound"
	boxService "github.com/sagernet/sing-box/adapter/service"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
)

type hotFixture struct {
	adapter.Outbound
	tag      string
	failAt   *adapter.StartStage
	stages   []adapter.StartStage
	closes   int
	listener net.Listener
}

func (*hotFixture) Type() string  { return "fixture" }
func (f *hotFixture) Tag() string { return f.tag }
func (f *hotFixture) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	f.stages = append(f.stages, stage)
	if stage == adapter.StartStateInitialize {
		scope.Add(func() error {
			f.closes++
			if f.listener != nil {
				return f.listener.Close()
			}
			return nil
		})
	}
	if stage == adapter.StartStateStart {
		var err error
		f.listener, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
	}
	if f.failAt != nil && *f.failAt == stage {
		return errors.New("fixture start failure")
	}
	return nil
}

type hotHarness struct {
	owner           adapter.Lifecycle
	create          func(string) error
	remove          func(string) error
	get             func(string) (*hotFixture, bool)
	corruptSnapshot func()
	newest          *hotFixture
	failAt          *adapter.StartStage
}

func newHotHarness(kind string) *hotHarness {
	h := new(hotHarness)
	ctx, logger := context.Background(), log.NewNOPFactory().Logger()
	makeOwner := func(tag string) *hotFixture {
		h.newest = &hotFixture{tag: tag, failAt: h.failAt}
		return h.newest
	}
	switch kind {
	case "inbound":
		r := inbound.NewRegistry()
		inbound.Register[option.StubOptions](r, "fixture", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ option.StubOptions) (adapter.Inbound, error) {
			return makeOwner(tag), nil
		})
		m := inbound.NewManager(r, endpoint.NewManager(nil))
		h.owner, h.remove = m, m.Remove
		h.create = func(tag string) error { return m.Create(ctx, nil, logger, tag, "fixture", nil) }
		h.get = func(tag string) (*hotFixture, bool) {
			f, ok := m.Get(tag)
			if !ok {
				return nil, false
			}
			return f.(*hotFixture), true
		}
		h.corruptSnapshot = func() { m.Inbounds()[0] = nil }
	case "endpoint":
		r := endpoint.NewRegistry()
		endpoint.Register[option.StubOptions](r, "fixture", func(_ context.Context, _ adapter.Router, _ log.ContextLogger, tag string, _ option.StubOptions) (adapter.Endpoint, error) {
			return makeOwner(tag), nil
		})
		m := endpoint.NewManager(r)
		h.owner, h.remove = m, m.Remove
		h.create = func(tag string) error { return m.Create(ctx, nil, logger, tag, "fixture", nil) }
		h.get = func(tag string) (*hotFixture, bool) {
			f, ok := m.Get(tag)
			if !ok {
				return nil, false
			}
			return f.(*hotFixture), true
		}
		h.corruptSnapshot = func() { m.Endpoints()[0] = nil }
	case "service":
		r := boxService.NewRegistry()
		boxService.Register[option.StubOptions](r, "fixture", func(_ context.Context, _ log.ContextLogger, tag string, _ option.StubOptions) (adapter.Service, error) {
			return makeOwner(tag), nil
		})
		m := boxService.NewManager(r)
		h.owner, h.remove = m, m.Remove
		h.create = func(tag string) error { return m.Create(ctx, logger, tag, "fixture", nil) }
		h.get = func(tag string) (*hotFixture, bool) {
			f, ok := m.Get(tag)
			if !ok {
				return nil, false
			}
			return f.(*hotFixture), true
		}
		h.corruptSnapshot = func() { m.Services()[0] = nil }
	}
	return h
}

func TestHotLifecycleRollbackPublicationAndListenerReuse(t *testing.T) {
	for _, kind := range []string{"inbound", "endpoint", "service"} {
		t.Run(kind, func(t *testing.T) {
			h := newHotHarness(kind)
			if err := h.create("same"); err != nil {
				t.Fatal(err)
			}
			accepted := h.newest
			if err := h.create("same"); err == nil || h.newest != accepted {
				t.Fatal("unstarted duplicate constructed or replaced an owner")
			}
			h.corruptSnapshot()
			scope := adapter.NewScope(t.Context(), log.NewNOPFactory().Logger())
			defer scope.Close()
			for _, stage := range adapter.ListStartStages {
				if err := scope.Start(kind, h.owner, stage); err != nil {
					t.Fatal(err)
				}
				if kind == "endpoint" && stage == adapter.StartStateStart {
					if err := h.owner.(*endpoint.Manager).StartEndpoint(accepted); err != nil {
						t.Fatal(err)
					}
				}
			}
			if !reflect.DeepEqual(accepted.stages, adapter.ListStartStages) {
				t.Fatal("startup stages or snapshot isolation changed", accepted.stages)
			}
			for _, stage := range adapter.ListStartStages {
				h.failAt = &stage
				if err := h.create("same"); err == nil {
					t.Fatal("failed candidate accepted")
				}
				failed := h.newest
				current, ok := h.get("same")
				if !ok || current != accepted || accepted.closes != 0 || failed.closes != 1 {
					t.Fatal("failed candidate disturbed publication or cleanup")
				}
				if failed.listener != nil {
					assertListenerReusable(t, failed.listener.Addr().String())
				}
			}
			h.failAt = nil
			oldAddress := accepted.listener.Addr().String()
			if err := h.create("same"); err != nil {
				t.Fatal(err)
			}
			replacement := h.newest
			current, ok := h.get("same")
			if !ok || current != replacement || accepted.closes != 1 || replacement.closes != 0 {
				t.Fatal("replacement ownership changed")
			}
			assertListenerReusable(t, oldAddress)
			if err := h.remove("same"); err != nil {
				t.Fatal(err)
			}
			if _, ok := h.get("same"); ok || replacement.closes != 1 {
				t.Fatal("removal retained owner")
			}
			assertListenerReusable(t, replacement.listener.Addr().String())
			if !errors.Is(h.remove("same"), os.ErrInvalid) {
				t.Fatal("missing removal accepted")
			}
			if err := scope.Close(); err != nil {
				t.Fatal(err)
			}
			if accepted.closes != 1 || replacement.closes != 1 {
				t.Fatal("removed owners closed twice")
			}
		})
	}
}

func assertListenerReusable(t *testing.T, address string) {
	t.Helper()
	l, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal("listener retained", err)
	}
	l.Close()
}
