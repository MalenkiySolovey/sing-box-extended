package endpoint

import (
	"context"
	"os"
	"slices"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	E "github.com/sagernet/sing/common/exceptions"
)

var _ adapter.EndpointManager = (*Manager)(nil)

type Manager struct {
	registry      adapter.EndpointRegistry
	access        sync.Mutex
	startAccess   sync.Mutex
	scope         *adapter.Scope
	stage         adapter.StartStage
	endpoints     []adapter.Endpoint
	endpointByTag map[string]adapter.Endpoint
}

func NewManager(registry adapter.EndpointRegistry) *Manager {
	return &Manager{
		registry:      registry,
		endpointByTag: make(map[string]adapter.Endpoint),
	}
}

func (m *Manager) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	m.startAccess.Lock()
	defer m.startAccess.Unlock()
	m.access.Lock()
	m.scope, m.stage = scope, stage
	if stage == adapter.StartStateStart {
		m.access.Unlock()
		return nil
	}
	endpoints := slices.Clone(m.endpoints)
	m.access.Unlock()
	for _, endpoint := range endpoints {
		name := "endpoint/" + endpoint.Type() + "[" + endpoint.Tag() + "]"
		err := scope.Start(name, endpoint, stage)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) StartEndpoint(endpoint adapter.Endpoint) error {
	m.startAccess.Lock()
	defer m.startAccess.Unlock()
	return m.scope.Start("endpoint/"+endpoint.Type()+"["+endpoint.Tag()+"]", endpoint, adapter.StartStateStart)
}

func (m *Manager) Endpoints() []adapter.Endpoint {
	m.access.Lock()
	defer m.access.Unlock()
	return slices.Clone(m.endpoints)
}

func (m *Manager) Get(tag string) (adapter.Endpoint, bool) {
	m.access.Lock()
	defer m.access.Unlock()
	endpoint, found := m.endpointByTag[tag]
	return endpoint, found
}

func (m *Manager) Remove(tag string) error {
	m.startAccess.Lock()
	defer m.startAccess.Unlock()
	m.access.Lock()
	owner, found := m.endpointByTag[tag]
	if !found {
		m.access.Unlock()
		return os.ErrInvalid
	}
	delete(m.endpointByTag, tag)
	m.endpoints = slices.DeleteFunc(m.endpoints, func(it adapter.Endpoint) bool { return it == owner })
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
	previous, loaded := m.endpointByTag[tag]
	m.access.Unlock()
	if loaded && scope == nil {
		return E.New("duplicate endpoint tag: ", tag)
	}
	endpoint, err := m.registry.Create(ctx, router, logger, tag, outboundType, options)
	if err != nil {
		return err
	}
	if scope != nil {
		name := "endpoint/" + endpoint.Type() + "[" + endpoint.Tag() + "]"
		for _, startStage := range adapter.ListStartStages[:stage+1] {
			if err := scope.Start(name, endpoint, startStage); err != nil {
				return E.Errors(err, scope.Remove(endpoint))
			}
		}
		if loaded {
			if err := scope.Remove(previous); err != nil {
				return E.Errors(err, scope.Remove(endpoint))
			}
		}
	}
	m.access.Lock()
	defer m.access.Unlock()
	if loaded {
		m.endpoints = slices.DeleteFunc(m.endpoints, func(it adapter.Endpoint) bool { return it == previous })
	}
	m.endpoints = append(m.endpoints, endpoint)
	m.endpointByTag[tag] = endpoint
	return nil
}
