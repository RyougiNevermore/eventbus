package eventbus_test

import (
	"fmt"
	"github.com/aacfactory/cluster"
	"github.com/rs/xid"
	"sync"
)

type TestRegistration struct {
	NodeId_   string                  `json:"nodeId,omitempty"`
	NodeName_ string                  `json:"nodeName,omitempty"`
	Id_       string                  `json:"id,omitempty"`
	Group_    string                  `json:"group,omitempty"`
	Name_     string                  `json:"name,omitempty"`
	Status_   cluster.ServiceStatus   `json:"status,omitempty"`
	Protocol_ string                  `json:"protocol,omitempty"`
	Address_  string                  `json:"address,omitempty"`
	Tags_     []string                `json:"tags,omitempty"`
	Meta_     cluster.Meta            `json:"meta,omitempty"`
	TLS_      cluster.RegistrationTLS `json:"tls,omitempty"`
}

func (s TestRegistration) NodeId() (nodeId string) {
	nodeId = s.NodeId_
	return
}

func (s TestRegistration) NodeName() (nodeName string) {
	nodeName = s.NodeName_
	return
}

func (s TestRegistration) Id() (id string) {
	id = s.Id_
	return
}

func (s TestRegistration) Group() (group string) {
	group = s.Group_
	return
}

func (s TestRegistration) Name() (name string) {
	name = s.Name_
	return
}

func (s TestRegistration) Status() (status cluster.Status) {
	status = s.Status_
	return
}

func (s TestRegistration) Protocol() (protocol string) {
	protocol = s.Protocol_
	return
}

func (s TestRegistration) Address() (address string) {
	address = s.Address_
	return
}

func (s TestRegistration) Tags() (tags []string) {
	tags = s.Tags_
	return
}

func (s TestRegistration) Meta() (meta cluster.Meta) {
	meta = s.Meta_
	return
}

func (s TestRegistration) TLS() (registrationTLS cluster.RegistrationTLS) {
	return s.TLS_
}

func NewTestDiscovery() cluster.ServiceDiscovery {

	return &TestDiscovery{
		lock:            new(sync.Mutex),
		registrationMap: make(map[string]cluster.Registration),
	}
}

type TestDiscovery struct {
	lock            *sync.Mutex
	registrationMap map[string]cluster.Registration
}

func (d *TestDiscovery) Publish(group string, name string, protocol string, address string, tags []string, meta cluster.Meta, registrationTLS cluster.RegistrationTLS) (registration cluster.Registration, err error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	registration = TestRegistration{
		NodeId_:   fmt.Sprintf("%s:%s", group, name),
		NodeName_: fmt.Sprintf("%s:%s", group, name),
		Id_:       xid.New().String(),
		Group_:    group,
		Name_:     name,
		Status_:   cluster.ServiceStatusRunning,
		Protocol_: protocol,
		Address_:  address,
		Tags_:     tags,
		Meta_:     meta,
		TLS_:      registrationTLS,
	}

	d.registrationMap[registration.Id()] = registration

	return
}

func (d *TestDiscovery) UnPublish(registration cluster.Registration) (err error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	delete(d.registrationMap, registration.Id())

	return
}

func (d *TestDiscovery) Get(group string, name string, tags ...string) (registration cluster.Registration, has bool, err error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	for _, stored := range d.registrationMap {
		if stored.Group() == group && stored.Name() == name {
			if tags == nil || len(tags) == 0 {
				registration = stored
				has = true
				return
			}
			mapped := 0
			for _, tag := range tags {
				if stored.Tags() == nil || len(stored.Tags()) == 0 {
					break
				}
				for _, st := range stored.Tags() {
					if tag == st {
						mapped++
					}
				}
			}
			if mapped == len(tags) {
				registration = stored
				has = true
				return
			}
		}
	}

	return
}

func (d *TestDiscovery) GetALL(group string, name string, tags ...string) (registrations []cluster.Registration, has bool, err error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	registrations = make([]cluster.Registration, 0, 1)
	for _, stored := range d.registrationMap {
		if stored.Group() == group && stored.Name() == name {
			if tags == nil || len(tags) == 0 {
				registrations = append(registrations, stored)
				continue
			}
			mapped := 0
			for _, tag := range tags {
				if stored.Tags() == nil || len(stored.Tags()) == 0 {
					break
				}
				for _, st := range stored.Tags() {
					if tag == st {
						mapped++
					}
				}
			}
			if mapped == len(tags) {
				registrations = append(registrations, stored)
				continue
			}
		}
	}

	has = len(registrations) > 0

	return
}
