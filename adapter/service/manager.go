package service

import (
	"context"
	"os"
	"slices"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	E "github.com/sagernet/sing/common/exceptions"
)

var _ adapter.ServiceManager = (*Manager)(nil)

type Manager struct {
	registry     adapter.ServiceRegistry
	access       sync.Mutex
	startAccess  sync.Mutex
	scope        *adapter.Scope
	stage        adapter.StartStage
	services     []adapter.Service
	serviceByTag map[string]adapter.Service
}

func NewManager(registry adapter.ServiceRegistry) *Manager {
	return &Manager{
		registry:     registry,
		serviceByTag: make(map[string]adapter.Service),
	}
}

func (m *Manager) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	m.startAccess.Lock()
	defer m.startAccess.Unlock()
	m.access.Lock()
	m.scope, m.stage = scope, stage
	services := slices.Clone(m.services)
	m.access.Unlock()
	for _, service := range services {
		name := "service/" + service.Type() + "[" + service.Tag() + "]"
		err := scope.Start(name, service, stage)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) Services() []adapter.Service {
	m.access.Lock()
	defer m.access.Unlock()
	return slices.Clone(m.services)
}

func (m *Manager) Get(tag string) (adapter.Service, bool) {
	m.access.Lock()
	service, found := m.serviceByTag[tag]
	m.access.Unlock()
	return service, found
}

func (m *Manager) Remove(tag string) error {
	m.startAccess.Lock()
	defer m.startAccess.Unlock()
	m.access.Lock()
	owner, found := m.serviceByTag[tag]
	if !found {
		m.access.Unlock()
		return os.ErrInvalid
	}
	delete(m.serviceByTag, tag)
	m.services = slices.DeleteFunc(m.services, func(it adapter.Service) bool { return it == owner })
	scope := m.scope
	m.access.Unlock()
	if scope != nil {
		return scope.Remove(owner)
	}
	return nil
}

func (m *Manager) Create(ctx context.Context, logger log.ContextLogger, tag string, serviceType string, options any) error {
	m.startAccess.Lock()
	defer m.startAccess.Unlock()
	m.access.Lock()
	scope, stage := m.scope, m.stage
	previous, loaded := m.serviceByTag[tag]
	m.access.Unlock()
	if loaded && scope == nil {
		return E.New("duplicate service tag: ", tag)
	}
	service, err := m.registry.Create(ctx, logger, tag, serviceType, options)
	if err != nil {
		return err
	}
	if scope != nil {
		name := "service/" + service.Type() + "[" + service.Tag() + "]"
		for _, startStage := range adapter.ListStartStages[:stage+1] {
			if err := scope.Start(name, service, startStage); err != nil {
				return E.Errors(err, scope.Remove(service))
			}
		}
		if loaded {
			if err := scope.Remove(previous); err != nil {
				return E.Errors(err, scope.Remove(service))
			}
		}
	}
	m.access.Lock()
	defer m.access.Unlock()
	if loaded {
		m.services = slices.DeleteFunc(m.services, func(it adapter.Service) bool { return it == previous })
	}
	m.services = append(m.services, service)
	m.serviceByTag[tag] = service
	return nil
}
