package inbound

import (
	"context"
	"os"
	"slices"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	E "github.com/sagernet/sing/common/exceptions"
)

var _ adapter.InboundManager = (*Manager)(nil)

type Manager struct {
	registry     adapter.InboundRegistry
	endpoint     adapter.EndpointManager
	access       sync.Mutex
	startAccess  sync.Mutex
	scope        *adapter.Scope
	stage        adapter.StartStage
	inbounds     []adapter.Inbound
	inboundByTag map[string]adapter.Inbound
}

func NewManager(registry adapter.InboundRegistry, endpoint adapter.EndpointManager) *Manager {
	return &Manager{
		registry:     registry,
		endpoint:     endpoint,
		inboundByTag: make(map[string]adapter.Inbound),
	}
}

func (m *Manager) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	m.startAccess.Lock()
	defer m.startAccess.Unlock()
	m.access.Lock()
	m.scope, m.stage = scope, stage
	inbounds := slices.Clone(m.inbounds)
	m.access.Unlock()
	for _, inbound := range inbounds {
		name := "inbound/" + inbound.Type() + "[" + inbound.Tag() + "]"
		err := scope.Start(name, inbound, stage)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) Inbounds() []adapter.Inbound {
	m.access.Lock()
	defer m.access.Unlock()
	return slices.Clone(m.inbounds)
}

func (m *Manager) Get(tag string) (adapter.Inbound, bool) {
	m.access.Lock()
	inbound, found := m.inboundByTag[tag]
	m.access.Unlock()
	if found {
		return inbound, true
	}
	return m.endpoint.Get(tag)
}

func (m *Manager) Remove(tag string) error {
	m.startAccess.Lock()
	defer m.startAccess.Unlock()
	m.access.Lock()
	owner, found := m.inboundByTag[tag]
	if !found {
		m.access.Unlock()
		return os.ErrInvalid
	}
	delete(m.inboundByTag, tag)
	m.inbounds = slices.DeleteFunc(m.inbounds, func(it adapter.Inbound) bool { return it == owner })
	scope := m.scope
	m.access.Unlock()
	if scope != nil {
		return scope.Remove(owner)
	}
	return nil
}

func (m *Manager) Create(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, outboundType string, options any) error {
	m.startAccess.Lock()
	defer m.startAccess.Unlock()
	m.access.Lock()
	scope, stage := m.scope, m.stage
	previous, loaded := m.inboundByTag[tag]
	m.access.Unlock()
	if loaded && scope == nil {
		return E.New("duplicate inbound tag: ", tag)
	}
	inbound, err := m.registry.Create(ctx, router, logger, tag, outboundType, options)
	if err != nil {
		return err
	}
	if scope != nil {
		name := "inbound/" + inbound.Type() + "[" + inbound.Tag() + "]"
		for _, startStage := range adapter.ListStartStages[:stage+1] {
			if err := scope.Start(name, inbound, startStage); err != nil {
				return E.Errors(err, scope.Remove(inbound))
			}
		}
		if loaded {
			if err := scope.Remove(previous); err != nil {
				return E.Errors(err, scope.Remove(inbound))
			}
		}
	}
	m.access.Lock()
	defer m.access.Unlock()
	if loaded {
		m.inbounds = slices.DeleteFunc(m.inbounds, func(it adapter.Inbound) bool { return it == previous })
	}
	m.inbounds = append(m.inbounds, inbound)
	m.inboundByTag[tag] = inbound
	return nil
}
